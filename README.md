twitter_stream_reader
=====================

Twitter_stream_reader

Este é um leitor do "stream" do twitter que filtra de acordo com algumas
palavras e alguns outros critérios e salva os tweets em uma instância do
ElasticSearch.

Você pode encontrar um texto-tutorial sobre como montar seu servidor do
ElasticSearch no link abaixo:

http://polignu.org/artigo/twitter-api-elasticsearch-e-kibana-analisando-rede-social

Requerimentos (bibliotecas python):
* TwitterAPI
* elasticsearch
* logging
* rfc822
* datetime
* numbers
* time
* subprocess

Obs.: O código considera que o serviço do ElasticSearch está rodando no
mesmo sistema operacional que o próprio código.

Comentários, sugestões e melhorias são bem vindas, tanto no código, quanto
no post, quanto na documentação aqui no twitter mesmo.
