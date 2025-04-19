import os
from association import Association
from basket import Basket

# Veri dizini ve dosya öneki
directory = os.path.join("data1", "data")
prefix = "igcdgz"

def get_fixed_months():
    """2023 Nisan'dan 2024 Temmuz'a kadar olan ayları döndürür."""
    start_year, start_month = 2023, 4  # Başlangıç tarihi
    end_year, end_month = 2024, 6  # Bitiş tarihi

    months = []  # Ayları saklamak için liste
    year, month = start_year, start_month  # İlk yıl ve ay

    # Başlangıç ve bitiş arasındaki tüm ayları listeye ekler
    while (year < end_year) or (year == end_year and month <= end_month):
        months.append((year, month))
        if month == 12:  # Aralıkta bir sonraki yıla geçer
            year += 1
            month = 1
        else:  # Diğer aylarda sadece ayı bir arttırır
            month += 1

    return months

def set_basket(year, month):
    """Bir ay için Association kurallarını ve basket çıktısını işler."""
    # Belirtilen yıl ve ay için Association sınıfı ile ilişkilendirme kuralları oluşturulur
    association = Association(directory, year, month, prefix)
    association.init()  # Association sınıfını başlatır ve ilişkileri yükler

    # Eğer ilişkiler başarıyla yüklenmişse, Basket sınıfını kullanarak işleme yapılır
    if association.association is not None and not association.association.empty:
        basket = Basket(association.association)  # Basket nesnesi oluşturulur
        df = association.basket  # Sepet verisi
        result_df = basket.process(df, association.association)  # Sepet verilerine işlem yapılır
        return result_df
    else:
        # İlişkilendirme kuralları bulunamazsa, hata mesajı basılır
        print(f"{year}-{month:02d} için kural seti oluşturulamadı.")
        return None

def main():
    """Ana fonksiyon: Her ayı sırayla işler."""
    for year, month in get_fixed_months():  # Belirtilen tarih aralığında her ayı işler
        print(f"\n--- İşleniyor: {year}-{month:02d} ---")  # İşlenen ayın bilgisini yazdırır
        set_basket(year, month)  # Set basket fonksiyonunu çağırarak işlemi başlatır

if __name__ == "__main__":
    # Ana fonksiyonu çalıştırır
    main()
