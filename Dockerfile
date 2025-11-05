FROM python:3.11-alpine
WORKDIR /application

COPY requirements.txt /application/
RUN pip install -r requirements.txt

COPY ./*.py /application/

COPY frontend /application/frontend

CMD ["python", "-u", "./server.py"]