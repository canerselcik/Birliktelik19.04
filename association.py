import json
import pandas as pd
import os
from pymongo import errors
from fpgrowth_py import fpgrowth  # FPGrowth algoritmasını kullanabilmek için gerekli
from mongo import Mongo  # MongoDB ile iletişim kurmak için Mongo sınıfı

class Association:
    def __init__(self, directory, year, month, prefix):
        # Başlangıçta gerekli parametreler alınarak sınıfı başlatır
        self.directory = directory  # Dosyaların bulunduğu dizin
        self.year = year            # Yıl bilgisi
        self.month = month          # Ay bilgisi
        self.prefix = prefix        # Dosya adındaki prefix
        self.df = None              # Veri çerçevesi (DataFrame)
        self.basket = None          # Sepet verisi (Basket)
        self.association = None     # Association kuralları (Elde edilecek kurallar)

    def fileload(self, path):
        # JSON dosyasını yükler ve içerik tipi hakkında bilgi verir
        try:
            with open(path, "r", encoding="utf-8") as file:
                data = json.load(file)  # JSON verisini yükler
                print(f"Dosya yüklendi: {path}, içerik tipi: {type(data)}")
                return data
        except (FileNotFoundError, json.JSONDecodeError) as e:
            # Dosya bulunamazsa veya geçersiz JSON verisi olursa hata mesajı verir
            print(f"Dosya yükleme hatası: {e}")
            return []

    def getFileList(self):
        # Dosya adlarını oluşturur (Özellikle yıl ve ay bilgisini içeren dosyalar)
        fileList = [f"{self.prefix}_orderdetail_{self.year}_{self.month}.json"]
        return fileList

    def getData(self):
        # Dosyaları okur ve JSON verilerini toplar
        data = []
        print("Dizin içindeki dosyalar:")
        for filename in self.getFileList():  # Dosya listesi döngüsü
            path = os.path.join(self.directory, filename)
            if os.path.isfile(path):  # Dosya mevcutsa
                temp = self.fileload(path)  # Dosyayı yükler
                if len(temp) > 0:
                    data.extend(temp)  # Veriyi toplar
                else:
                    print(f"Dosya '{path}' boş.")
            else:
                print(f"Dosya '{path}' mevcut değil.")
        return data

    def setDataFrame(self):
        # JSON verisini pandas DataFrame'e çevirir
        data = self.getData()
        if len(data) > 0:
            self.df = pd.DataFrame(data)  # Veriyi DataFrame'e dönüştürür
            print(f"Veri çerçevesi oluşturuldu, toplam {len(self.df)} satır.")
        else:
            print("Veri çerçevesi oluşturulamadı; veri yok.")

    def setBasketGroup(self):
        # Siparişleri ve müşterileri gruplar (Basket: Sipariş ve müşteri bazında ürün grubu)
        if self.df is not None:
            # 'OrderCode' ve 'CustomerCode' bazında gruplama yaparak her sipariş için bir sepet oluşturur
            self.basket = self.df.groupby(["OrderCode", "CustomerCode"]).agg(
                {"ProductCode": lambda s: list(set(s))}).reset_index()
            print(f"Sepet grubu oluşturuldu, toplam {len(self.basket)} grup.")
        else:
            print("Veri çerçevesi yüklenmedi; sepet grubu oluşturulamadı.")

    def setRules(self):
        # FPGrowth algoritması ile ürünler arası ilişki kurallarını çıkarır
        if self.basket is not None:
            print("FPGrowth algoritması çalıştırılıyor...")

            try:
                # FPGrowth algoritmasını çalıştırarak frekanslı itemsetleri ve kuralları elde ederiz
                freqItemset, rules = fpgrowth(
                    self.basket["ProductCode"].values, minSupRatio=0.005, minConf=0.005)
                
                # minSupRatio: Minimum destek oranı (örneğin 0.005, yani %0.5'lik frekans)
                #Destek, bir ürünün veya ürün grubunun veri setinde ne kadar yaygın olduğunu gösterir. Yani, bir ürünün sepetlerde ne kadar sık göründüğünü ölçer.
                #Örnek: 1000 siparişte 50'sinde "Ürün A" var, o zaman destek oranı %5 olur.
                #Destek oranını düşürmek: Daha fazla kural, ancak daha az güvenilir kurallar,Destek oranını arttırmak: Daha az kural, ancak daha güçlü ve yaygın ilişkiler.
                # minConf: Minimum güven oranı (örneğin 0.005, yani %0.5'lik güven)
                #Güven, bir ürün alındığında, diğer ürünün alınma olasılığını ölçer. Yani, bir ürün A alındığında, ürün B'nin de alınma ihtimali nedir?
                #Örnek: "Ürün A" alınan 100 siparişte, 80'inde "Ürün B" de alınmışsa, güven oranı %80 olur.
                #Güven oranını düşürmek: Daha fazla kural, ancak daha az güvenilir ilişkiler,Güven oranını arttırmak: Daha az kural, ancak daha güçlü ve güvenilir ilişkiler.

                print(f"Kurallar oluşturuldu, toplam {len(rules)} kural.")
                return freqItemset, rules
            except Exception as e:
                # Algoritma çalıştırılırken herhangi bir hata oluşursa yakalar ve hata mesajı verir
                print(f"FPGrowth hatası: {e}")
                return None, None
        else:
            print("Sepet verisi yüklenemedi.")
            return None, None

    def setTable(self):
        # FPGrowth algoritmasından çıkan kuralları tabloya dönüştürür
        freqItemset, rules = self.setRules()
        if rules is not None and len(rules) > 0:
            # Kurallar varsa bunları bir pandas DataFrame'e çeviririz
            self.association = pd.DataFrame(
                rules, columns=["Basket", "Next_Product", "Proba"])
            
            # Kuralları güven değerine göre azalan sırada sıralar
            self.association = self.association.sort_values(by="Proba", ascending=False)

            # Sepet ve sonraki ürünleri liste formatında düzenler
            self.association["Basket"] = self.association["Basket"].apply(lambda x: list(x))
            self.association["Next_Product"] = self.association["Next_Product"].apply(lambda x: list(x))

            print("Kural tablosu başarıyla oluşturuldu.")
        else:
            print("Kurallar oluşturulamadı veya kural yok.")

    def update_basket_data(self, data):
        # MongoDB'ye verileri günceller veya yeni veri ekler
        db = Mongo()
        try:
            collection = "association"
            for item in data:
                basket = item.get("Basket")
                if basket:
                    basket_id = list(basket)
                    where = {"Basket": basket}
                    update = {
                        "$set": {"Basket": basket},
                        '$push': {'Proba': item["Proba"]},
                        "$addToSet": {"Next_Product": {"$each": list(item["Next_Product"])}}
                    }

                    result = db.updateOne(
                        collection, where, update, upsert=True)
                    if result.matched_count:
                        print(f"Sepet {basket_id} başarıyla güncellendi.")
                    elif result.upserted_id:
                        print(f"Yeni sepet eklendi: {basket_id}")
        except errors.PyMongoError as e:
            print(f"MongoDB hatası: {e}")
        finally:
            db.mongoClose()

    def init(self):
        # Tüm işlemleri başlatan metod
        self.setDataFrame()
        self.setBasketGroup()
        self.setTable()
        
        if self.association is not None and not self.association.empty:
            data = self.association.to_dict(orient="records")
            self.update_basket_data(data)  # MongoDB veritabanına güncelleme yapar
        else:
            print("Association tablosu oluşturulamadı; işlem tamamlanamadı.")
