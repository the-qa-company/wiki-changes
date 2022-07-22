# wikibase-changes

Tool to extract diff from a wikibase instance

## Required

- Java 17
- Lombok plugin (Dev only)

## Install

You can download the latest version in the [release section](https://github.com/the-qa-company/wiki-changes/releases).


You can also clone and compile the jar by yourself with these commands

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
