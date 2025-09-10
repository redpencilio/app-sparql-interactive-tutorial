# SPARQL Interactive Tutorial

This is an interactive tutorial to guide you through SPARQL, a W3C standard designed for querying RDF graphs.

## Installation

### Requirements

- [Docker](https://docs.docker.com/engine/install/)
- [git-lfs](https://github.com/git-lfs/git-lfs/wiki/Installation)

### Steps

1. Clone this repository
2. Run `docker compose up -d` into the root folder
3. The database will be ingesting some data. That may take a while; about 15 mins.
   To track progress, do `docker compose logs -f --tail=10 database`. When you see:
   ```
   (...)
        Server online at 1111 (pid 1)
   (...)
   ```
   That means it's ready.
4. Go to http://localhost
5. Enjoy!

## URLs

* [SPARQL Tutorial](http://localhost)
* [SPARQL Endpoint](http://localhost:8890/sparql)
* [Virtuoso Conductor](http://localhost:8890/conductor/)
