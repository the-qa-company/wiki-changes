param(
    $minlength,
    $maxlength,
    $server,
    $sparql,
    $date,
    $updater,
    $delta,
    [Switch]
    $syncrequest,
    [Switch]
    $today,
    [Switch]
    $color,
    [Switch]
    $help,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" -UseDoubleMinus "com.the_qa_company.wikidatachanges.WikidataChangesUpdater" -RequiredParameters $PSBoundParameters