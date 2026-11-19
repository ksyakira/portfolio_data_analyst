'''
=================================================
Graded Challenge 2

Nama  : Khoizuron Syakira
Batch : CODA-023-RMT

Program ini adalah aplikasi Shopping Cart sederhana. Program ini memungkinkan pengguna untuk menambah, menghapus, melihat daftar barang di 
keranjang belanja, serta menghitung total harga.
=================================================
'''

# Variabel utama untuk menampung daftar barang di keranjang
keranjang_belanja = []

'''
Fungsi ini ditujukan untuk menambahkan item barang baru beserta harganya
ke dalam list keranjang_belanja.
'''
def tambah_barang(nama, harga):
    item = {'nama': nama, 'harga': harga}
    keranjang_belanja.append(item)
    print(f'Barang "{nama}" berhasil dimasukkan ke keranjang.\n')


def hapus_barang(nama):
    '''
    Fungsi ini ditujukan untuk menghapus item barang dari keranjang_belanja
    berdasarkan nama barang yang diinput pengguna.
    '''
    ditemukan = False
    for item in keranjang_belanja:
        if item['nama'].lower() == nama.lower():
            keranjang_belanja.remove(item)
            print(f'Barang "{item["nama"]}" berhasil dihapus di keranjang belanja.\n')
            ditemukan = True
            break
    
    if not ditemukan:
        print(f'Barang "{nama}" tidak ditemukan di keranjang belanja.\n')


def tampilkan_barang():
    '''
    Fungsi ini ditujukan untuk menampilkan seluruh daftar barang yang tersimpan
    di dalam keranjang_belanja.
    '''
    if not keranjang_belanja:
        print("Keranjang belanja kamu masih kosong.\n")
    else:
        print("Barang di Keranjang:")
        for index, item in enumerate(keranjang_belanja):
            print(f"{index}. {item['nama']} - Rp {item['harga']:.2f}")
        print()


def hitung_total():
    '''
    Fungsi ini ditujukan untuk menghitung total harga seluruh barang di keranjang_belanja.
    '''
    total = sum(item['harga'] for item in keranjang_belanja)
    print(f"Total belanja: Rp {total:.2f}\n")


def main():
    '''
    Fungsi ini merupakan menu utama untuk menjalankan user-interface aplikasi.
    Menggunakan While Loop dan Conditional IF agar pengguna dapat interaktif 
    memilih menu hingga memilih Exit.
    '''
    print("Selamat Datang di Keranjang Belanja Toko Syakira!\n")
    
    while True:
        print('''Menu:
1. Menambah Barang
2. Hapus Barang
3. Tampilkan Barang di Keranjang
4. Lihat Total Belanja
5. Exit''')
        
        pilihan = input("Pilihan: ")
        
        if pilihan == '1':
            nama = input("Masukan nama barang: ")
            try:
                harga = float(input("Masukan harga: "))
                tambah_barang(nama, harga)
            except ValueError:
                print("Harga harus berupa angka! Coba lagi ya.\n")
                
        elif pilihan == '2':
            nama = input("Masukan nama barang yang ingin dihapus: ")
            hapus_barang(nama)
            
        elif pilihan == '3':
            tampilkan_barang()
            
        elif pilihan == '4':
            hitung_total()
            
        elif pilihan == '5':
            print("Sampai Jumpa! Terima kasih sudah belanja di Toko Syakira.")
            break
            
        else:
            print("Pilihannya salah. Coba lagi ya.\n")


# Menjalankan fungsi utama program
if __name__ == '__main__':
    main()