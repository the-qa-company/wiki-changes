# wikibase-changes

Tool to extract diff from a wikibase instance

- [wikibase-changes](#wikibase-changes)
  - [Required](#required)
  - [Install](#install)
    - [Installation](#installation)
      - [Scoop](#scoop)
      - [Brew](#brew)
      - [Command Line Interface](#command-line-interface)
  - [Usage](#usage)
  - [Steps](#steps)
    - [Read updates](#read-updates)
    - [Sites HDT build](#sites-hdt-build)
    - [Create new indexed HDT from the sites and source hdts](#create-new-indexed-hdt-from-the-sites-and-source-hdts)
      - [Diff HDT](#diff-hdt)
      - [Cat HDTs](#cat-hdts)
      - [Create index](#create-index)
  - [Up to date script](#up-to-date-script)
  - [Diff experiment](#diff-experiment)
  - [Publications](#publications)

## Required

- Java 17
- Lombok plugin (Dev only)

## Install

### Installation

#### Scoop

You can install WikiChanges using the [Scoop package manager](http://scoop.sh/).

You need to add the [`the-qa-company` bucket](https://github.com/the-qa-company/scoop-bucket), and then you will be able to install the `wikichanges` manifest, it can be done using these commands

```powershell
# Add the-qa-company bucket
scoop bucket add the-qa-company https://github.com/the-qa-company/scoop-bucket.git
# Install WikiChanges CLI
scoop install wikichanges
```

#### Brew

You can install WikiChanges using the [Brew package manager](http://brew.sh/).

You can install is using this command

```bash
brew install the-qa-company/tap/wikichanges
```

#### Command Line Interface

If you don't have access to Brew or Scoop, the WikiChanges command line interface is available in [the releases page](https://github.com/the-qa-company/wiki-changes/releases) under the file `wikidata-changes.zip`. By extracting it, you can a bin directory that can be added to your path.

You can also clone and compile the jar by yourself with these commands

**For now, you need to use a custom library to fix an issue with the Java 9 dependencies here: https://github.com/ate47/hdt-java/tree/diff_fix, clone it and run `mvn install -DskipTests` to isntall it**

```bash
git clone https://github.com/the-qa-company/wiki-changes.git
cd wiki-changes
./gradlew shadowJar
mv ./build/libs/wikidata-changes-*-all.jar wiki-changes.jar
```

## Usage

```bash
java -Xmx4g -jar ./wiki-changes.jar --date UTC_DATE_TOSEARCH --hdtsource YOUR_PREVIOUS_DUMP
```

- `UTC_DATE_TOSEARCH` is the date to search the changes, you can type `java -jar ./wiki-changes.jar -T` to get the current date.
- `YOUR_PREVIOUS_DUMP` is your previous HDT dump to diff/cat to the changes.
- `-Xmx4g` is here to specify the memory to allocate to the process, here 4GB, the diff/cat computation and the changes hdt creation can require a lot of memory.

the result will be in `cache/result.hdt`.


## Steps

The wiki-changes is composed of multiple steps, you can ignore with an option.

### Read updates

Find the updates in the wiki and put it in the `cache/sites` directory, will also write deleted subjects into `cache/deletedSubjects` (one line/subject).

**Ignore option**: `-C` or `--nonewcache`.

### Sites HDT build

Create an HDT from the `cache/sites` directory to `cache/sites.hdt` .

**Ignore option**: `-H` or `--nonewhdt`.

### Create new indexed HDT from the sites and source hdts

Use the HDT in `cache/sites.hdt` and the hdt specified in `--hdtsource` to create a new HDT to `cache/result.hdt`.

**Ignore option**: Don't specify the `--hdtsource`

#### Diff HDT

Remove all the subjects of the sites from the source HDT and create a new HDT without them in `cache/diff.hdt`.

**Ignore option**: `-u` or `--nodiff`

#### Cat HDTs

Cat the `cache/sites.hdt` HDT and the `cache/diff.hdt` into `cache/result.hdt`.

**Ignore option**: `-p` or `--notcat`

#### Create index

Create the `cache/result.hdt.index.v1-1` file for the `cache/result.hdt` HDT.

**Ignore option**: `-I` or `--noindex`

## Up to date script

```bash
scripts/uptodate.sh (base hdt) (output hdt)
```

This script will take the base hdt, go back to a particular date and create the updated HDT.

**Usage**:

- Get a start HDT with an UTC date associate to it, you can type `java -jar ./wiki-changes.jar -T` to get the current date.
- Write the UTC date to the `date.txt` file
- (If required) You can add more/less ram to the Java process by editing the `scripts/uptodate.sh` config section
- run the command `scripts/uptodate.sh (base hdt) (output hdt)`, the process will create an up to date HDT from the base HDT with a backtrace to the date contained in `date.txt`. The end HDT will be written in the *base hdt* file and then copy to the *output hdt* file before waiting for the next iteration. The `date.txt` file will be updated to the current date.

## Diff experiment

A test to compute the diff efficiency is usable.

You need first to get a Wikidata HDT, then compute the delta HDT using the delta tool,

replace the date by the date of your dump and simple by the flavor of the dump (simple=truthy, full=all)

```powershell
java -cp .\wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesDelta --date 2023-10-31T00:40:00Z -f simple -m 0 -S 10000
```

It'll create a file `delta.df`.

This file format is usable to create HDTs using the qEndpoint CLI in the branch [dev_dl_file](https://github.com/the-qa-company/qEndpoint/tree/dev_dl_file) with rdf2hdt.

Once you have you HDTs (dump + delta), you can run the different experiment using:

```powershell
# Create delete bitmap
java -cp wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesCompute bitmap delta.hdt wikidata.hdt bitmap.bin
# Div a hdt into x pieces (here 7)
java -cp wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesCompute div delta.hdt 7
# Run DiffCat with all the pieces (same names as output)
java -cp wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesCompute mergediff wikidata.hdt bitmap.bin delta.hdt 7
# Compute diff of a HDT and a bitmap
java -cp wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesCompute diffonly wikidata.hdt bitmap.bin
# move the result to diff.hdt
mv changes.hdt diff.hdt
# Compute cat of two HDTs
java -cp wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesCompute catonly diff.hdt delta.hdt
# Compute diffcat of 2 HDTs and a bitmap
java -cp wiki-changes.jar com.the_qa_company.wikidatachanges.WikidataChangesCompute catdiffonly wikidata.hdt bitmap.bin delta.hdt
```

## Publications

- Willerval Antoine, Dennis Diefenbach, and Pierre Maret. "Easily setting up a local Wikidata SPARQL endpoint using the qEndpoint." Workshop ISWC (2022). [PDF](https://www.researchgate.net/publication/364321138_Easily_setting_up_a_local_Wikidata_SPARQL_endpoint_using_the_qEndpoint)
- Willerval Antoine, Dennis Diefenbach, Angela Bonifati. "Generate and Update Large HDT RDF Knowledge Graphs on Commodity Hardware" ESWC (2024). [PDF](https://www.researchgate.net/publication/379507028_Generate_and_Update_Large_HDT_RDF_Knowledge_Graphs_on_Commodity_Hardware)
