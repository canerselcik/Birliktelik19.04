from pymongo import MongoClient, errors

class Mongo:
    def __init__(self, dsn=None, dbname=None):
        # MongoDB bağlantı bilgilerini alır (varsayılan localhost)
        self.dsn = dsn or "mongodb://localhost:27017"
        self.dbname = dbname or "genismarty2"
        self.client = None
        self.db = None
        self.mongoConnect()  # Bağlantıyı başlatır

    def mongoConnect(self):
        # MongoDB’ye bağlanmayı dener
        try:
            self.client = MongoClient(self.dsn)
            self.db = self.client[self.dbname]
            print("MongoDB'ye başarılı bir şekilde bağlanıldı.")
        except errors.ConnectionFailure as e:
            print(f"MongoDB bağlantı hatası: {e}")
            self.client = None
            self.db = None

    def insertOne(self, collection, document):
        # Tek bir belge (document) ekler
        try:
            result = self.db[collection].insert_one(document)
            return result.inserted_id
        except errors.PyMongoError as e:
            print(f"MongoDB insertOne hatası: {e}")
            return None

    def insertMany(self, collection, documents):
        # Birden fazla belge ekler
        try:
            result = self.db[collection].insert_many(documents)
            return result.inserted_ids
        except errors.PyMongoError as e:
            print(f"MongoDB insertMany hatası: {e}")
            return None

    def findOne(self, collection, query, projection=None, sort=None):
        # Tek bir belge arar ve getirir
        try:
            return self.db[collection].find_one(query, projection, sort)
        except errors.PyMongoError as e:
            print(f"MongoDB findOne hatası: {e}")
            return None

    def find(self, collection, query, projection=None, sort=None, limit=None, skip=None):
        # Belirli kriterlerle çoklu belge arar ve liste olarak döner
        try:
            cursor = self.db[collection].find(query, projection)
            if sort:
                cursor = cursor.sort(sort)
            if skip:
                cursor = cursor.skip(skip)
            if limit:
                cursor = cursor.limit(limit)
            return list(cursor)
        except errors.PyMongoError as e:
            print(f"MongoDB find hatası: {e}")
            return None

    def updateOne(self, collection, query, update, upsert=False):
        # Tek bir belgeyi günceller
        try:
            return self.db[collection].update_one(query, update, upsert=upsert)
        except errors.PyMongoError as e:
            print(f"MongoDB updateOne hatası: {e}")
            return None

    def updateMany(self, collection, query, update, upsert=False):
        # Birden fazla belgeyi günceller
        try:
            return self.db[collection].update_many(query, update, upsert=upsert)
        except errors.PyMongoError as e:
            print(f"MongoDB updateMany hatası: {e}")
            return None

    def deleteOne(self, collection, query):
        # Tek bir belgeyi siler
        try:
            return self.db[collection].delete_one(query)
        except errors.PyMongoError as e:
            print(f"MongoDB deleteOne hatası: {e}")
            return None

    def deleteMany(self, collection, query):
        # Belirli kriterlere uyan tüm belgeleri siler
        try:
            return self.db[collection].delete_many(query)
        except errors.PyMongoError as e:
            print(f"MongoDB deleteMany hatası: {e}")
            return None

    def mongoClose(self):
        # MongoDB bağlantısını kapatır
        if self.client:
            self.client.close()
            print("MongoDB bağlantısı kapatıldı.")
