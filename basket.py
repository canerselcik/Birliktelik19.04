import pandas as pd

class Basket:
    def __init__(self, association_data):
        # Başlangıçta ilişkilendirme verisini (association rules) alır
        # Bu veriler, ürünler arasındaki ilişkiyi ve olasılıkları içerir
        self.association = association_data

    def compute_next_best_product(self, basket_el):
        """
        Sepetteki bir ürün için en olası sonraki ürünü tahmin eder.
        Bu metod, belirli bir ürün (veya ürün grubu) için ilişkilendirme tablosuna bakarak
        en yüksek olasılıkla birlikte gelen ürünü döndürür.
        
        Parametre:
        basket_el: Sepetteki tek bir ürün (veya ürün grubu)
        
        Dönen Değer:
        next_pdt: Sepete eklenebilecek en olası ürün
        proba: Bu ürünün sepete eklenme olasılığı
        """
        basket_el = list(basket_el)  # basket_el set'ini listeye çevir
        for k in basket_el:
            k = {k}  # Tek bir ürünü set formatında tutar
            # Sepette sadece bu ürün olan satırları filtreler
            matching_rows = self.association[self.association["Basket"].apply(lambda x: set(x) == k)]
            if not matching_rows.empty:
                # Eşleşme varsa, önerilen ürünü ve olasılığını döner
                next_pdt = list(matching_rows["Next_Product"].values[0])[0]
                if next_pdt not in basket_el:
                    # Sepette olmayan bir ürünse, öneriyi döndürür
                    proba = matching_rows["Proba"].values[0]
                    return next_pdt, proba
        # Eğer eşleşme bulunmazsa, 0 döner (başka bir ürün önerisi yok)
        return 0, 0

    def find_next_product(self, basket):
        """
        Sepetteki her ürün için, en olası sonraki ürünü ve olasılığını bulur.
        
        Parametre:
        basket: Sepetteki ürünlerin listesini içerir
        
        Dönen Değer:
        list_next_pdt: Her ürün için önerilen bir sonraki ürün listesi
        list_proba: Her ürün için önerilen ürünün olasılıkları
        """
        list_next_pdt = []  # Sepetteki her ürün için önerilen ürünleri tutar
        list_proba = []  # Sepetteki her ürün için olasılıkları tutar
        for product in basket["ProductCode"]:
            el = set(product)  # Ürünü set formatına çevirir
            # Ürünle birebir eşleşen satırı arar
            matching_rows = self.association[self.association["Basket"].apply(lambda x: set(x) == el)]
            if not matching_rows.empty:
                next_pdt = list(matching_rows["Next_Product"].values[0])[0]
                proba = matching_rows["Proba"].values[0]
            else:
                # Eşleşme yoksa, alternatif bir tahmin yapılır
                next_pdt, proba = self.compute_next_best_product(product)
            # Listeye öneriyi ve olasılığı ekler
            list_next_pdt.append(next_pdt)
            list_proba.append(proba)
        return list_next_pdt, list_proba

    def process(self, df, association):
        """
        Ana işlem: Verilen veri çerçevesine (df) tahmin edilen "next product" ve "probability" sütunlarını ekler.
        
        Parametreler:
        df: Sepet verilerini içeren DataFrame (ürünler)
        association: Ürünler arası ilişkilendirme kurallarını içeren DataFrame
        
        Dönen Değer:
        df: Yeni sütunlar eklenmiş veri çerçevesi
        """
        # Eğer association tablosu boşsa, işlem yapılmaz
        if association is None or association.empty:
            print("Association tablosu boş; işlem gerçekleştirilemiyor.")
            return df
        # Association verisini sınıf içerisinde tutar
        self.association = association
        # Sepetteki her ürün için, olası bir sonraki ürünü bulur
        next_products, probabilities = self.find_next_product(df)
        # Yeni sütunlar olarak tahmin edilen ürünleri ve olasılıkları ekler
        df["Next_Product"] = next_products
        df["Probability"] = probabilities
        return df
