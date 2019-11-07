FROM python:3.6

WORKDIR /usr/notebook-ui
COPY . /usr/notebook-ui

# install Jupyter via pip
RUN pip3 install --upgrade pip 
RUN pip3 install jupyter

# install required libraries
RUN pip3 install ipywidgets 	&&\
	pip3 install pandas 		&&\
	pip3 install astropy 		&&\
	pip3 install pyvo			&&\
	pip3 install cadcutils		&&\
	pip3 install cadctap		&&\
	pip3 install git+https://github.com/glue-viz/glue-jupyter.git &&\
	pip3 install ipyvuetify		&&\
	pip3 install astroquery		&&\
	pip3 install drawSvg 		&&\
	pip3 install flake8		    &&\	
	jupyter nbextension enable --sys-prefix --py widgetsnbextension

RUN pip3 install appmode	&& \
	jupyter nbextension     enable --py --sys-prefix appmode && \
	jupyter serverextension enable --py --sys-prefix appmode
				
EXPOSE 8888

CMD ["jupyter-notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--NotebookApp.token=''"]
