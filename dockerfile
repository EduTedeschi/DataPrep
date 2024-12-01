FROM python:3.12

WORKDIR /DataPrep

COPY . .

RUN pip install -r requirements.txt

CMD ["python","./code/main.py"]