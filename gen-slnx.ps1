Set-Location $PSScriptRoot
$lines = @('<Solution>')
$lines += '  <Folder Name="/src/">'
Get-ChildItem src -Directory | ForEach-Object {
    $p = "src/$($_.Name)/$($_.Name).csproj"
    if (Test-Path $p) { $lines += "    <Project Path=`"$p`" />" }
}
$lines += '  </Folder>'
$lines += '  <Folder Name="/tests/">'
Get-ChildItem tests -Directory | ForEach-Object {
    $p = "tests/$($_.Name)/$($_.Name).csproj"
    if (Test-Path $p) { $lines += "    <Project Path=`"$p`" />" }
}
$lines += '  </Folder>'
$lines += '</Solution>'
$lines | Out-File -FilePath Kuestenlogik.Surgewave.Connectors.slnx -Encoding utf8
Write-Host "Generated Kuestenlogik.Surgewave.Connectors.slnx with $($lines.Count) lines"
