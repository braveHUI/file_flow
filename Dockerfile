FROM jfloff/alpine-python
ADD . /code/das_flow
WORKDIR /code
RUN pip install -r das_flow/requirements.txt
CMD ["python","das_flow/check_run.py"]
