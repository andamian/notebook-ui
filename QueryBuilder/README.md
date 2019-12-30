### Run QueryBuilder locally without Docker:
1.Git clone this repository
2.```pip install jupyter ```
3.```pip install -r requirements.txt```
4.Enable ipywidgets ```jupyter nbextension enable --sys-prefix --py widgetsnbextension```
5.Install and enable appmode(Optional)
```
pip install appmode
jupyter nbextension     enable --py --sys-prefix appmode
jupyter serverextension enable --py --sys-prefix appmode
```
3.Run ```jupyter notebook```

### A development environment for QueryBuilder can be created with Docker:

1.Install Docker

2.Git clone this repository

3.Cd into QueryBuilder directory

4.```docker build -t querybuilder-image -f Dockerfile . ```

5.```docker run --rm -d -p 8888:8888 --name [name] querybulder-image```

6.Browse to http://localhost:8888
