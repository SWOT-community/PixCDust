
URL conda-forge: https://github.com/conda-forge/pixcdust-feedstock

URL Pypi: https://pypi.org/project/pixcdust/


Update the version number in pyproject.toml then :

## Tag new version on github
```
git tag <tagname>
git push <origin-github> --tags
```

##  Deploy to Pypi
```
poetry config pypi-token.pypi <MY_PYPI_TOKEN>
poetry build
poetry publish
```


## Deploy to Conda 
It automatically update from pypi (except dependencies). 
If the dependencies changes, you need to update the meta.yaml file: https://github.com/conda-forge/pixcdust-feedstock/blob/main/recipe/meta.yaml
