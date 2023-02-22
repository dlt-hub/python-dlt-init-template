# How this repo works
1. `dlt init <source> <destination>` clones this repo, it uses the version of the `dlt` as a git tag to clone
2. if the `<source>` is one of the variants ie. `chess` then the variant (ie. `chess.py`) is used as pipeline template, if not the `pipeline.py` will be used (after renaming to the <source>)
2. if `--generic` options is passed the `pipeline_generic.py` template is used
3. it modifies the script by importing the right destination and using it in the pipeline
4. it will rename all `dlt.source` and `dlt.resource` function defs and calls.
5. it copies the .gitignore, the pipeline script created above and other files in `TEMPLATE_FILES` variable
6. it will create the `secrets.toml` and `config.toml` for the sources and destination in the script
7. it will add the right dlt extra to `requirements.txt`

# How to update template safely
1. Create a branch with your modifications
2. Push it to github
3. Use `dlt init` with `--branch` option to test your branch
4. Only propose PRs to master if you are happy with the result

TODO: automated test that will initialize all the variants and generic templates in the branch