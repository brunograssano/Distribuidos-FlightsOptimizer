FROM rabbitmq:3.12.6-management-alpine
RUN apk update && apk add curl