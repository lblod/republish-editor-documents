FROM ruby:2.5
WORKDIR /app
COPY Gemfile /app
RUN bundle
ENV INPUT_PATH="." OUTPUT_PATH="."
COPY . /app
CMD "/app/republish.rb"