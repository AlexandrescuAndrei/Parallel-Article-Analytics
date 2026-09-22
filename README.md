# Parallel-Article-Analytics

A multithreaded **article-processing and analytics pipeline implemented in Java**, designed to process collections of JSON articles concurrently and generate structured indexes, statistics, keyword frequencies, and summary reports.

The application distributes input files dynamically between multiple worker threads, parses JSON article batches using Jackson, detects duplicate articles globally, synchronizes processing phases using a `CyclicBarrier`, and performs most aggregation locally inside each worker before combining the partial results into the final output.

The project focuses on practical parallel programming: workload distribution, thread synchronization, concurrent data structures, avoiding unnecessary contention, local aggregation, shared-state coordination, and deterministic output generation.

The data-processing pipeline also includes article deduplication, filtering by language and category, English keyword extraction with stop-word removal, author and category statistics, date-based sorting, and generation of multiple output files.

## Processing Pipeline

The application receives three command-line arguments:

- the number of worker threads
- a file describing the article input files
- a configuration file describing languages, categories, and stop words

The overall execution is divided into several logical stages.

First, configuration files are loaded and all article-file paths are inserted into a shared concurrent work queue.

The requested number of worker threads is then created.

Each worker repeatedly removes one file path from the queue, parses the JSON contained in that file, and stores its articles in a private local buffer.

During this first phase, workers also update global counters used for duplicate detection.

After all input files have been consumed, the workers synchronize at a barrier.

Only after every worker has finished the loading phase does duplicate filtering begin.

This separation is important because deciding whether an article is unique requires knowing how many times every UUID and every title occurred across the complete input dataset.

After duplicate filtering and per-article analysis are complete, the workers synchronize again.

The main thread waits for all workers to terminate, merges their local results, sorts the final article collection, and writes the output files.

## Dynamic Work Distribution

Input files are distributed using a shared `ConcurrentLinkedQueue`.

Before the worker threads start, every article-file path is inserted into this queue.

Each worker repeatedly executes a non-blocking `poll` operation to obtain the next available file.

This creates dynamic scheduling rather than assigning a fixed subset of files to every thread beforehand.

If some files contain more articles than others, a worker that finishes a smaller file can immediately take another task from the queue.

This reduces the effect of uneven input sizes and allows the workload to be distributed according to the actual processing speed of the workers.

Because `ConcurrentLinkedQueue` is designed for concurrent access, multiple threads can safely remove tasks without an additional explicit lock around the queue.

## Parallel JSON Parsing

Each input file contains a JSON array of articles.

Every worker owns its own Jackson `ObjectMapper` and parses the files that it obtains from the shared task queue.

The JSON objects are mapped to instances of the `Article` class.

The model includes fields such as:

- UUID
- title
- author
- URL
- text
- publication timestamp
- language
- categories

`Article` uses Jackson annotations to map JSON property names to the internal Java fields.

Unknown JSON properties are ignored, allowing the parser to focus only on the data needed by the application.

Keeping an independent parser inside each worker avoids sharing parser state between threads while allowing several JSON files to be processed simultaneously.

## Global Duplicate Detection

Duplicate detection is one of the parts of the application that requires global knowledge of the dataset.

An article is considered valid only when both its UUID and its title occur at most once across all input articles.

Two concurrent hash maps are used during the loading phase:

- one counting UUID occurrences
- one counting title occurrences

Whenever a worker reads an article, it increments the corresponding counters using `ConcurrentHashMap.merge`.

This allows all threads to update the shared registers safely.

The important detail is that articles cannot be filtered immediately when they are first encountered.

For example, the first occurrence of a UUID initially appears unique, but another worker may later encounter the same UUID in a completely different input file.

For this reason, the first processing phase only collects articles and counts identifiers.

Actual filtering occurs only after every worker has finished reading its files.

## Barrier Synchronization

A `CyclicBarrier` is shared by all worker threads.

The first barrier separates two logically different phases:

1. reading articles and building global duplicate counters
2. filtering articles and extracting statistics

Every worker must complete phase one before any worker starts making final decisions about uniqueness.

After the barrier is passed, the global UUID and title counters contain information from the entire dataset.

Each worker can then safely inspect the counters for the articles stored in its local buffer.

A second barrier is reached after the filtering and analysis stage.

This phase-based structure makes the dependency between global preprocessing and local computation explicit.

The project therefore uses synchronization not for every individual operation, but at the boundaries where one stage depends on the complete results of another stage.

## Filtering Unique Articles

Each worker keeps the articles it originally parsed in a local `rawBuffer`.

After the first barrier, the worker revisits this buffer.

For every article, it checks the globally computed occurrence counts of its UUID and title.

The article is accepted only if:

- its UUID does not appear more than once
- its title does not appear more than once

Articles that pass both checks are added to the worker's local list of valid articles.

Only valid articles contribute to the language, category, author, and keyword statistics generated during the next stage.

This ensures that duplicated entries do not affect the final analytics.

## Thread-Local Aggregation

Most statistics are not updated directly in shared global maps while the worker threads are running.

Instead, every worker maintains its own local structures.

These include:

- language-to-article mappings
- category-to-article mappings
- keyword counters
- author counters
- language statistics
- category statistics
- valid article lists

This design reduces synchronization overhead.

Rather than forcing every processed article to contend for access to the same shared aggregation structures, workers perform the majority of their updates independently.

After all workers terminate, the main thread combines their partial results.

This resembles a **MapReduce-style processing strategy**.

Workers independently process their assigned data and produce partial aggregates, after which those results are reduced into global structures.

## Language Processing

The configuration specifies a set of target languages.

When a valid article is processed, its language is checked against this set.

If the language is relevant, the worker stores the article UUID in the local list associated with that language.

It also increments the local count for that language.

After all worker results are merged, the application generates one output file per language represented in the final language map.

Before being written, the UUIDs associated with each language are sorted.

This produces deterministic language-based article indexes.

## Category Processing

Articles may belong to multiple categories.

The application checks each article category against the configured set of target categories.

For accepted categories, a normalized representation is created.

Normalization includes:

- removing commas
- trimming surrounding whitespace
- replacing spaces with underscores

For example, a category containing spaces can therefore become suitable for use as an output filename.

A per-article `HashSet` tracks normalized categories that have already been processed.

This prevents the same normalized category from contributing multiple times for a single article if duplicate category entries are present in that article's data.

The article UUID is then added to the local category mapping, and the category counter is incremented.

As with languages, final UUID lists are sorted before being written to their corresponding files.

## English Keyword Extraction

Keyword analysis is performed only for valid articles whose language is `english` and whose language is part of the configured target language set.

The article text is converted to lowercase and split on whitespace.

Each token is cleaned by removing every character that is not an English lowercase letter.

Empty results are ignored.

Words that occur in the configured stop-word set are also discarded.

The implementation then stores valid words in a `HashSet` for the current article.

As a result, a word contributes at most once per article, even if it appears repeatedly inside that article.

The final keyword counter therefore represents how many processed English articles contain each keyword rather than the total number of raw token occurrences.

After all worker-local keyword maps are merged, keywords are sorted primarily by descending count.

Ties are resolved alphabetically.

The resulting values are written to `keywords_count.txt`.

## Stop-Word Filtering

The stop-word list is loaded during configuration initialization.

Stop words are converted to lowercase when loaded.

Since article text is also converted to lowercase before token processing, matching is case-independent.

This removes common words that would otherwise dominate the keyword output while contributing relatively little information about article content.

The combination of lowercasing, alphabetic cleaning, stop-word filtering, and per-document deduplication creates a simple document-level keyword frequency analysis.

## Author Statistics

Every valid article with a non-null author contributes to the worker's local author counter.

After processing ends, local counters are merged into the global author statistics map.

The final report identifies the author associated with the highest number of valid articles.

When several keys have the same count, the helper used by the report resolves the tie lexicographically.

The same top-value logic is also reused for language, category, and keyword statistics.

## Language and Category Statistics

In addition to generating article lists, the application records how many valid articles belong to each configured language and category.

Each worker maintains separate local counters.

These are combined after all worker threads finish.

The report then determines:

- the most frequent language
- the most frequent category

The values are based only on valid articles that survive duplicate filtering.

This means duplicated articles do not inflate the statistics.

## Article Ordering

All valid articles from every worker are merged into a single `masterList`.

Before the main article output is generated, this list is sorted.

Publication timestamps are ordered in descending lexicographic order.

When two articles have the same publication value, their UUIDs are used as the secondary ordering criterion.

UUID ties are resolved in ascending lexicographic order.

Articles with missing publication values are placed after articles that have a timestamp.

The resulting order is used to generate `all_articles.txt`.

Each output line contains the article UUID followed by its publication timestamp.

## Most Recent Article

Because the final article list is sorted by publication timestamp in descending order, the first valid article becomes the source for the `most_recent_article` report entry.

The application outputs its publication value and URL.

If there are no valid articles, the report writes a placeholder instead.

This reuses the already sorted result rather than performing an additional pass to find the most recent entry.

## Final Reduction

After all workers have completed their parallel processing, the main thread performs the reduction stage.

For each worker, it:

- adds the number of raw articles read to the total
- appends valid articles to the master list
- merges language UUID lists
- merges category UUID lists
- sums keyword counts
- sums author counts
- sums language statistics
- sums category statistics

Because this phase happens after the worker threads have joined, these final maps can use ordinary `HashMap` and `ArrayList` structures.

They no longer require concurrent updates.

This is another important design decision: synchronization is used only where parallel access actually occurs.

## Generated Files

The application generates several types of output.

`all_articles.txt` contains the UUID and publication timestamp of every valid article after global sorting.

Language-specific `.txt` files contain sorted UUIDs of valid articles belonging to each processed target language.

Category-specific `.txt` files contain sorted UUIDs associated with each processed target category.

`keywords_count.txt` contains the extracted English keywords together with their document-level counts.

`reports.txt` summarizes the complete execution.

The report includes:

- number of duplicate articles removed
- number of unique articles
- author with the largest article count
- most common language
- most common category
- most recent article
- top English keyword

These outputs transform the original distributed JSON input into several indexes and aggregate views.

## Duplicate Report

The application tracks the total number of raw articles read across all workers.

After processing, the number of accepted unique articles is known from the merged master list.

The number written as `duplicates_found` is calculated as the difference between these two values.

Therefore, the report represents the number of input article entries eliminated by the duplicate-filtering rules.

## Deterministic Output

Parallel execution can process files and articles in different orders depending on thread scheduling.

The project avoids allowing that scheduling order to determine important output ordering.

UUID lists are explicitly sorted before being written.

The master article list is explicitly sorted by publication timestamp and UUID.

Keywords are explicitly ordered by count and then alphabetically.

Top-statistic tie breaking is also performed lexicographically.

This allows the parallel computation stage to remain nondeterministic internally while producing predictable final files.

## Concurrency Design

The project uses different concurrency techniques depending on the type of data involved.

`ConcurrentLinkedQueue` is used for dynamic distribution of file-processing tasks.

`ConcurrentHashMap` is used for the global occurrence counters that must be updated while several threads are active.

`CyclicBarrier` coordinates phase transitions that require all threads to have completed an earlier stage.

Worker-local `HashMap`, `HashSet`, and `ArrayList` instances are used whenever sharing is unnecessary.

Finally, the global reduction occurs after `Thread.join`, when no worker can modify its local data anymore.

This combination limits the amount of shared mutable state and keeps synchronization concentrated around the parts of the algorithm that actually require it.

## Worker Lifecycle

Each worker follows the same lifecycle.

It first consumes article files from the shared queue.

For every article encountered, it stores the object locally and updates the global UUID and title occurrence registers.

It then waits at the first barrier.

Once all workers have reached that point, the worker examines its local articles, removes globally duplicated entries, and extracts statistics from the valid ones.

Its raw article buffer can then be released.

The worker reaches the second barrier and eventually terminates.

The main thread joins all worker threads before accessing and merging their local results.

This provides a clear separation between parallel computation and the final sequential reduction/output stage.

## Configuration Loading

The application loads several auxiliary files before worker execution begins.

The configuration identifies files containing:

- target languages
- target categories
- stop words

The article descriptor file provides the JSON article files that must be processed.

Relative paths are resolved against the directories of the descriptor files.

This makes it possible for the processing logic to work with datasets organized across multiple files instead of requiring one monolithic JSON input.

## Article Model

`Article.java` provides the data model used by Jackson.

The class maps external JSON names such as `uuid`, `title`, `author`, `url`, `text`, `published`, `language`, and `categories` to internal Java fields.

Getter methods expose the values to the processing code.

The class is marked with `@JsonIgnoreProperties(ignoreUnknown = true)`, so additional fields in the input JSON do not prevent deserialization.

This keeps the model focused on the fields that are actually required by the analytics pipeline.

## Project Structure

The repository contains:

- `Tema1.java` — multithreaded processing pipeline, synchronization, duplicate detection, aggregation, analytics, sorting, and output generation
- `Article.java` — Jackson-compatible article data model
- `Makefile` — build, run, and cleanup commands
- `lib/` — Jackson JAR dependencies used for JSON processing
- `README.pdf` — original project documentation

The majority of the concurrency and data-processing logic is contained in `Tema1.java`.

## Build and Run

The repository includes a `Makefile` that configures the current directory and the JAR files from `lib/` as the Java classpath.

The project can be compiled with:

`make build`

The program can then be executed through:

`make run ARGS="<P> <articles_file> <config_file>"`

where `P` represents the number of worker threads.

Internally, the executable expects the equivalent Java invocation:

`java Tema1 <P> <art> <in>`

The Jackson dependencies required for JSON processing are already referenced through the `lib` directory by the Makefile.

Generated `.class` and `.txt` files can be removed using the included cleanup target.

## Technologies and Concepts

- Java
- Parallel programming
- Multithreading
- Concurrent data processing
- Producer-consumer-style work distribution
- Dynamic task scheduling
- `Thread`
- `Runnable`
- `Thread.join`
- `ConcurrentLinkedQueue`
- `ConcurrentHashMap`
- `CyclicBarrier`
- Thread synchronization
- Phase synchronization
- Shared-state coordination
- Thread-local aggregation
- MapReduce-style processing
- Reduction
- JSON processing
- Jackson
- `ObjectMapper`
- Concurrent duplicate detection
- Data deduplication
- Hash maps
- Hash sets
- Array lists
- Keyword extraction
- Stop-word filtering
- Document-level keyword frequency
- Language indexing
- Category indexing
- Statistical aggregation
- Sorting
- Deterministic output
- File I/O
- Dynamic workload balancing
- Java Collections Framework
- Makefiles
