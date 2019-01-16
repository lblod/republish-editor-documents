# service to republish editor-documents
Runs a script, looks for all docs which should have a publish status. Removes publication. Republish editor doc
Starts immedialty.

# Usage

docker-compose.yml example
```
version: "3.4"
services:
  republish:
    image: lblod/republish-editor-documents
    environment:
      ENDPOINT: 'http://virtuoso:8890/sparql'
      PUBLISHENDPOINT: 'http://publicatie'
      INPUT_PATH: '/data/input'
      OUTPUT_PATH: '/data/output'
    volumes:
      - './data/republish-editor-documents/output:/data/output'
      - './data/republish-editor-documents/input:/data/input' # not used yet
```