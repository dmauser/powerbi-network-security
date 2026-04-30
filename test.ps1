$machines = Import-Csv .\decommissioned-servers.csv

foreach ($m in $machines) {
  $resource = Get-AzResource `
    -ResourceType "Microsoft.HybridCompute/machines" `
    -Name $m.Name `
    -ErrorAction SilentlyContinue

  if ($resource) {
    Write-Host "Deleting Arc machine:" $m.Name
    Remove-AzResource -ResourceId $resource.ResourceId -Force
  }
}
``