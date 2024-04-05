param(

    $cache,
    $element,
    $wikiapi,
    [Switch]
    $today,
    $date,
    $flavor,
    [Switch]
    $flavorlist,
    [Switch]
    $nonewcache,
    [Switch]
    $deletecache,
    $maxtry,
    $sleeptry,
    [Switch]
    $nonewhdt,
    [Switch]
    $hdtload,
    $hdtsource,
    [Switch]
    $mapbitmap,
    [Switch]
    $deletesites,
    [Switch]
    $notcat,
    [Switch]
    $noindex,
    [Switch]
    $advupdate,
    [Switch]
    $help,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" -UseDoubleMinus "com.the_qa_company.wikidatachanges.WikidataChangesFetcher" -RequiredParameters $PSBoundParameters