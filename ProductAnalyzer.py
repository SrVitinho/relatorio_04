from database import Database
from writeAJson import writeAJson

class ProductAN:
    def __init__(self):
        self.db = Database(database="mercado", collection="compras")
        self.db.resetDatabase()

    def clientemaicaro(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": {"cliente": "$cliente_id", "data": "$data_compra"}, "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
            {"$sort": {"_id.data": 1, "total": -1}},
            {"$group": {"_id": "$_id.data", "cliente": {"$first": "$_id.cliente"}, "total": {"$first": "$total"}}},
            {"$limit": 1}
            ])
        writeAJson(result, "Cliente que mais gastou")

    def maisde1(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.descricao", "total": {"$sum": "$produtos.quantidade"}}},
            {"$match" : {"total" : {"$gt": 0}}},
            {"$sort": {"total": -1}}
        ])
        writeAJson(result, "Produtos que venderam mais que 1")

    def maisvendido(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.descricao", "total": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"total": -1}},
            {"$limit": 1}
        ])
        writeAJson(result, "Produto que mais vendeu")

    def total(self):
        result = self.db.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.descricao", "total": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"total": -1}}
        ])
        writeAJson(result, "TOTAL")