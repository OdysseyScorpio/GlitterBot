from json import JSONDecoder

import redis
import os
import settings


class Database(object):
    instance = None

    def __new__(cls):
        if not Database.instance:
            Database.instance = Database.__Database()
        return Database.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, **kwargs):
        return setattr(self.instance, name, kwargs)

    class __Database:

        decoder = JSONDecoder()

        def __init__(self):
            self.__db_connection = None
            self.__market = None

        def connect_db(self, version):

            if version in settings.API_DB_CONFIG:
                db_number = settings.API_DB_CONFIG[version]
            else:
                raise ValueError('Unknown API Version')

            try:
                ENV_GWP_DB_NAME = os.environ.get("ENV_GWP_DB_NAME")
                ENV_GWP_DB_PORT = os.environ.get("ENV_GWP_DB_PORT")
            except:
                ENV_GWP_DB_NAME = settings.DATABASE_IP
                ENV_GWP_DB_PORT = settings.DATABASE_PORT
                
            # Open and connect to the database, Decode responses in UTF-8 (default)
            db = redis.Redis(ENV_GWP_DB_NAME, ENV_GWP_DB_PORT, decode_responses=True, db=db_number)
            db.set_response_callback('GET', self.__parse_boolean_responses_get)
            db.set_response_callback('HGET', self.__parse_boolean_responses_get)
            db.set_response_callback('HGETALL', self.__parse_boolean_responses_hgetall)
            db.set_response_callback('HMGET', self.__parse_boolean_responses_hgetall)

            self.__db_connection = db
            self.__market = version

        @property
        def connection(self) -> redis.Redis:

            if self.__db_connection is None:
                raise ValueError('Please call connect_db first')

            return self.__db_connection

        @property
        def market(self) -> str:
            if self.__db_connection is None:
                raise ValueError('Please call connect_db first')
            return self.__market

        @property
        def pipeline(self):
            if self.__db_connection is None:
                raise ValueError('Please call connect_db first')

            pipe = self.__db_connection.pipeline()
            pipe.set_response_callback('GET', self.__parse_boolean_responses_get)
            pipe.set_response_callback('HGET', self.__parse_boolean_responses_get)
            pipe.set_response_callback('HGETALL', self.__parse_boolean_responses_hgetall)
            pipe.set_response_callback('HMGET', self.__parse_boolean_responses_hgetall)

            return pipe

        @classmethod
        def __try_auto_parse(cls, val):
            try:
                # Quick fixes for broken True/False strings in Redis
                if val == "False":
                    return False
                elif val == "True":
                    return True
                elif val == "None":
                    return None
                else:
                    val = cls.decoder.decode(val)
            except Exception:
                pass
            return val

        @classmethod
        def __parse_boolean_responses_hgetall(cls, response, **options):
            if not response:
                return {}

            for index in range(1, len(response), 2):
                response[index] = cls.__try_auto_parse(response[index])

            it = iter(response)
            return dict(zip(it, it))

        @classmethod
        def __parse_boolean_responses_get(cls, response, **options):
            if not response:
                return None

            return cls.__try_auto_parse(response)
