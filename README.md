# wikibase-changes

Tool to extract diff from a wikibase instance

## Required

- Java 17
- Lombok plugin (Dev only)

## Install

You can create the jar with this command
```bash
./gradlew shadowJar
mv ./build/libs/wikidata-changes-*-all.jar wiki-changes.jar
```

then run it with

```bash
java -jar ./wiki-changes.jar --date UTC_DATE_TOSEARCH --hdtsource YOUR_PREVIOUS_DUMP
```

- `UTC_DATE_TOSEARCH` is the date to search the changes, you can type `java -jar ./wiki-changes.jar -T` to get the current date
- `YOUR_PREVIOUS_DUMP` is your previous HDT dump to diff/cat to the changes

the result will be in `cache/result.hdt`.
