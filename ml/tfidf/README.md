# Optional offline tagger

This is not scheduled or deployed on the web host. Its model algorithm remains legacy code; the security maintenance session verified the image build, imports/compilation, dependency audit and `--help`, not a complete model-generation run. Validate it against synthetic local articles before enabling it.

Build from the **repository root**:

```sh
docker build -f ml/tfidf/Dockerfile -t yewreview-tagger .
docker run --rm yewreview-tagger --help
```

The Python 3.12 image runs as an unprivileged user, pins resolved packages in `requirements.lock.txt`, and contains neither configuration nor models. Edit direct dependencies in `requirements.txt` and regenerate the lock from a clean image when updating them. Packaging tools are pinned separately in the Dockerfile.

Supply `config.yml` only as a read-only runtime bind mount at `/app/ml/tfidf/config.yml`. Never use a build argument or copy a secret mount into an image layer. Remote MySQL requires a trusted CA file, certificate verification and hostname verification; mount the CA at the configured path. Only explicit `YEWREVIEW_ENVIRONMENT=local` with a local database host may use plaintext transport. Local development must use the isolated Compose database/API, without production credentials.

For authorized production SQL access from a Mac, follow the web project's Session Manager instructions, retain RDS TLS hostname verification and use a separately approved MySQL account. AWS tunnel permissions do not determine SQL grants. The tagger also needs the distinct service API token and service user ID; human credentials are rejected by the automatic-tagging API.

Model pickle files can execute code when loaded. Use only models generated locally from trusted code, never downloaded or user-supplied pickle files. Runtime state/configuration stays outside Git and image layers.
