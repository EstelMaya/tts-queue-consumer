FROM python:3.10-alpine
RUN pip install aiohttp aio_pika
EXPOSE 80
COPY . .
CMD ["python", "-u" , "server.py"]
