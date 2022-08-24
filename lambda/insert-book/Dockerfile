FROM public.ecr.aws/lambda/python:3.9

COPY app.py ./
COPY requirements.txt ./

RUN pip install -r requirements.txt

	
CMD ["app.handler"]
