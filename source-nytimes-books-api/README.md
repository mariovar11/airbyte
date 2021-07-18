# Nytimes Books Api Source 

This is the repository for the Nytimes Books Api source connector.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.io/integrations/sources/nytimes-books-api).

## Local development

### Prerequisites
* If you are using Python for connector development, minimal required version `= 3.7.0`
* Valid credentials (see the "Create credentials section for instructions)
TODO: _which languages and tools does a user need to develop on this connector? add them to the bullet list above_

### Iteration
TODO: _which commands should a developer use to run this connector locally?_

### Testing
#### Unit Tests
TODO: _how can a user run unit tests?_

#### Integration Tests
TODO: _how can a user run integration tests?_
_this section is currently under construction -- please reach out to us on Slack for help with setting up Airbyte's standard test suite_


### Locally running the connector docker image

First, make sure you build the latest Docker image:
```
docker build . -t airbyte/nytimes-books-api:dev
```

Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-nytimes-books-api:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-nytimes-books-api:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-nytimes-books-api:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-nytimes-books-api:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```

#### Create credentials
**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.io/integrations/sources/nytimes-books-api)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `spec.json` file. `secrets` is gitignored by default.

**If you are an Airbyte core member**, copy the credentials from Lastpass under the secret name `source nytimes-books-api test creds`
and place them into `secrets/config.json`.
