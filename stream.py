# -*- coding: utf-8 -*-
# Copyright (C) 2014, Diego Rabatone Oliveira
#
# Twitter_stream_reader is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Twitter_stream_reader is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Twitter_stream_reader. If not, see <http://www.gnu.org/licenses/>.

# Importando as bibliotecas

from TwitterAPI import TwitterAPI
import elasticsearch
import logging
import rfc822
import datetime
import numbers
import time
from subprocess import Popen, PIPE


def patch_tweet(d):
    """A API do twitter retorna as datas num formato que o elasticsearch
        não consegue reconhecer, então precisamos parsear a data para um
        formato que o ES entende, essa função faz isso.
    """
    if 'created_at' in d:
        # twitter uses rfc822 style dates. elasticsearch uses iso dates.
        # we translate twitter dates into datetime instances (pyes will
        # convert datetime into the right iso format understood by ES).
        new_date = datetime.datetime(*rfc822.parsedate(d['created_at'])[:6])
        d['created_at'] = new_date

    count_is_number = isinstance(d['retweet_count'], numbers.Number)
    if 'retweet_count' in d and not count_is_number:
        # sometimes retweet_count is a string instead of a number (eg. "100+"),
        # here we transform it to a number (an attribute in ES cannot have
        # more than one type).
        d['retweet_count'] = int(d['retweet_count'].rstrip('+')) + 1

    return d


def check_es_status():
    """ Essa é uma função que verifica se o serviço do ElasticSearch está
        operando e inicia-o ou reinicia-o caso seja necessário.
    """
    cmd = Popen(["service", "elasticsearch", "status"], stdout=PIPE)
    cmd_out, cmd_err = cmd.communicate()
    print(cmd_out)
    if "not running" in cmd_out:
        print "Elastic Search Not Running, trying to start it"
        cmd = Popen(["service", "elasticsearch", "start"], stdout=PIPE)
        cmd_out, cmd_err = cmd.communicate()
        print(cmd_out)
    time.sleep(15)

# get trace logger and set level
log_dir = "/var/log/elasticsearch/"
tracer = logging.getLogger('elasticsearch.trace')
tracer.setLevel(logging.WARN)
tracer.addHandler(logging.FileHandler(log_dir + 'trace.log'))
default_logger = logging.getLogger('Elasticsearch')
default_logger.setLevel(logging.WARN)
default_logger.addHandler(logging.FileHandler(log_dir + 'default.log'))

stream_log = logging.getLogger(__name__)
stream_log.setLevel(logging.INFO)
handler = logging.FileHandler('stream.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
stream_log.addHandler(handler)


es = elasticsearch.Elasticsearch(["192.168.0.171:9200"])

#Configure TwitterAPI keys
twitter_api = TwitterAPI(
    consumer_key='consumer_key',
    consumer_secret='consumer_secret',
    access_token_key='access_token',
    access_token_secret='access_token_secret')

#Set the language and words to filter on the stream
filters = {
    "language": ["pt"],
    "track":
    [
        "PSDBconversa",
        "VamosConversar",
        "aecio",
        "alckmin",
        "atrasado",
        "atraso",
        "aécio",
        "barbosa",
        "black block",
        "brasil",
        "brasília",
        "cabral",
        "chuchu",
        "civil",
        "congresso",
        "copa",
        "corrup",
        "cracolandia",
        "cracolândia",
        "davos",
        "deputado",
        "desempreg",
        "desvio",
        "dilma",
        "dirceu",
        "eduardo campos",
        "eleicao",
        "eleição",
        "estadao",
        "estadaodados",
        "estadio",
        "estádio",
        "federal",
        "fifa",
        "futebol",
        "genuino",
        "governador",
        "haddad",
        "imposto",
        "inflacao",
        "inflação",
        "joaquim barbosa",
        "joseserra_",
        "josé serra",
        "jovem",
        "juros",
        "justica",
        "justiça",
        "kassab",
        "ladrao",
        "ladrão",
        "lula",
        "maranhão",
        "mensalao",
        "mensaleiro",
        "mensalão",
        "militar",
        "ministerio",
        "ministr",
        "padilha",
        "pedrinhas",
        "petrobras",
        "pmdb",
        "policia",
        "polícia",
        "prefeit",
        "prefeitura",
        "president",
        "presidi",
        "presídi",
        "preço",
        "propina",
        "protesto",
        "psb",
        "psd",
        "psdb",
        "pt",
        "public",
        "públic",
        "renda",
        "rolezinho",
        "roseana",
        "sarney",
        "selecao",
        "seleção",
        "senado",
        "shopping",
        "silvamarina",
        "skaf",
        "stf",
        "suborno",
        "superavit",
        "taxa",
        "tre",
        "tse",
        "urna",
        "valke",
        "zé dirceu"
    ]
}


def errorLog(error):
    stream_log.info("--------------------------------------------")
    stream_log.info(time.ctime())
    if error:
        stream_log.error(error, exc_info=True)

while True:
    #creates the stream object
    stream = twitter_api.request('statuses/filter', filters)

    try:
        #Do the magic!
        #For each item in the stream (tweet data), save it on the elastisearch
        for item in stream.get_iterator():

            try:
                # Saving the tweet on the ES
                es.index(
                    index="tweets",
                    doc_type="tweet",
                    body=patch_tweet(item)
                )
            except Exception:
                errorLog(Exception)
                check_es_status()
                es = elasticsearch.Elasticsearch(["192.168.0.171:9200"])
                print ("Getting back to tweet recording")
    except Exception:
        errorLog(Exception)
        pass
