param(
    [Switch]
    $build
)



$prevPwd = $PWD

try {
    $base = (Get-Item $PSScriptRoot).parent
    Set-Location ($base.Fullname)
    
    if ($build) {
        .\gradlew shadowJar

        if (!$?) {
            return;
        }
    }

    $base = "build/package/wikidata-changes"

    # Delete previous builds
    Remove-Item -Recurse -Force -ErrorAction Ignore  "$base" > $null
    Remove-Item -Force -ErrorAction Ignore "$base.zip" > $null

    # Create structure
    New-Item "$base" -ItemType Directory > $null
    New-Item "$base/licenses" -ItemType Directory > $null
    New-Item "$base/bin" -ItemType Directory > $null
    New-Item "$base/lib" -ItemType Directory > $null

    # Binaries
    Copy-Item "build/libs/*-all.jar" "$base/lib/wikidata-changes.jar" > $null
    Copy-Item "cli/*" "$base/bin" > $null

    # License
    Copy-Item "README.md" "$base/README.md" > $null
    Copy-Item "LICENSE.md" "$base/licenses/license.md" > $null

    # Compress
    Compress-Archive -LiteralPath "$base" -DestinationPath "$base.zip" > $null

    Write-Host "Packaged to '$base.zip'"
}
finally {
    $prevPwd | Set-Location
}