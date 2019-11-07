FROM python:3.6

WORKDIR /usr/notebook-ui
COPY . /usr/notebook-ui

# install Jupyter via pip
RUN pip3 install --upgrade pip 
RUN pip3 install jupyter

# install required libraries
RUN pip3 install -r requirements.txt

# enable ipywidgets 
RUN jupyter nbextension enable --sys-prefix --py widgetsnbextension

# install and enable appmode 
RUN pip3 install appmode	&& \
	jupyter nbextension     enable --py --sys-prefix appmode && \
	jupyter serverextension enable --py --sys-prefix appmode

EXPOSE 8888

CMD ["jupyter-notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--NotebookApp.token=''"]
