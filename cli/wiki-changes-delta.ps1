param(
    $element,
    [switch]
    $today,
    $date,
    $wikiapi,
    $flavor,
    [switch]
    $flavorlist,
    $maxtry,
    $sleeptry,
    [Switch]
    $help,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" -UseDoubleMinus "com.the_qa_company.wikidatachanges.WikidataChangesDelta" -RequiredParameters $PSBoundParameters