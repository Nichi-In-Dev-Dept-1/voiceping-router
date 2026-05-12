[CmdletBinding()]
param(
  [string]$Region = "ap-northeast-1",
  [string]$AccountId = "807110418145",
  [string]$Repository = "voiceping-router",
  [string]$Cluster = "voiceping-router-cluster",
  [string]$Service = "voiceping-router-service",
  [string]$TaskDefFile = "ecs-task-def.json",
  [string]$ContainerName = "voiceping-router",
  [string]$Tag = ""
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$env:AWS_PAGER = ""

function Invoke-Step {
  param(
    [string]$Message,
    [scriptblock]$Action
  )

  Write-Host "`n==> $Message"
  & $Action
  if ($LASTEXITCODE -ne 0) {
    throw "Step failed: $Message"
  }
}

if (-not (Test-Path $TaskDefFile)) {
  throw "Task definition file not found: $TaskDefFile"
}

if (-not $Tag) {
  $Tag = Get-Date -Format "yyyyMMdd-HHmmss"
}

$registry = "$AccountId.dkr.ecr.$Region.amazonaws.com"
$imageLocal = "$Repository`:$Tag"
$imageRemote = "$registry/$Repository`:$Tag"

Invoke-Step -Message "ECR login" -Action {
  aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $registry
}

Invoke-Step -Message "Docker build ($imageLocal)" -Action {
  docker build -t $imageLocal .
}

Invoke-Step -Message "Docker tag ($imageRemote)" -Action {
  docker tag $imageLocal $imageRemote
}

Invoke-Step -Message "Docker push ($imageRemote)" -Action {
  docker push $imageRemote
}

Write-Host "`n==> Preparing task definition with new image"
$taskDef = Get-Content -Raw -Path $TaskDefFile | ConvertFrom-Json
$container = $taskDef.containerDefinitions | Where-Object { $_.name -eq $ContainerName } | Select-Object -First 1

if (-not $container) {
  throw "Container '$ContainerName' not found in $TaskDefFile"
}

$container.image = $imageRemote

$tempTaskDef = Join-Path $env:TEMP "ecs-task-def-$Tag.json"
$taskDef | ConvertTo-Json -Depth 30 | Out-File -FilePath $tempTaskDef -Encoding utf8

Invoke-Step -Message "Register ECS task definition" -Action {
  $script:registerOut = aws ecs register-task-definition --region $Region --cli-input-json "file://$tempTaskDef" --output json
}

$registerJson = $registerOut | ConvertFrom-Json
$taskDefinitionArn = $registerJson.taskDefinition.taskDefinitionArn

Invoke-Step -Message "Update ECS service to new task definition" -Action {
  aws ecs update-service --region $Region --cluster $Cluster --service $Service --task-definition $taskDefinitionArn --force-new-deployment | Out-Null
}

Invoke-Step -Message "Wait for service stabilization" -Action {
  aws ecs wait services-stable --region $Region --cluster $Cluster --services $Service
}

Write-Host "`n==> Getting current task endpoint"
$taskArn = aws ecs list-tasks --region $Region --cluster $Cluster --service-name $Service --desired-status RUNNING --query "taskArns[0]" --output text

if ($taskArn -and $taskArn -ne "None") {
  $eniId = aws ecs describe-tasks --region $Region --cluster $Cluster --tasks $taskArn --query "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value | [0]" --output text
  $publicIp = aws ec2 describe-network-interfaces --region $Region --network-interface-ids $eniId --query "NetworkInterfaces[0].Association.PublicIp" --output text

  Write-Host "`nDeployment complete"
  Write-Host "Image: $imageRemote"
  Write-Host "Task definition: $taskDefinitionArn"
  Write-Host "Task ARN: $taskArn"
  if ($publicIp -and $publicIp -ne "None") {
    Write-Host "HTTP: http://$publicIp`:3000/"
    Write-Host "WS:   ws://$publicIp`:3000"
  }
} else {
  Write-Host "`nDeployment complete"
  Write-Host "Image: $imageRemote"
  Write-Host "Task definition: $taskDefinitionArn"
  Write-Host "No running task found yet. Check ECS service events."
}
