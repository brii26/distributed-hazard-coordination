# Panduan Gaya Menulis

Status: disetujui penulis pada 2026-09-29; gunakan sebagai acuan gaya penulisan laporan.

## Sumber gaya

Panduan ini diturunkan dari empat sampel tulisan asli penulis (tidak disertakan di repositori):

- penjelasan sistem, 852 kata;
- narasi masalah teknis, 731 kata;
- argumentasi keputusan teknis, 686 kata; dan
- refleksi proyek, 616 kata.

Jangan mengambil salah ketik, singkatan percakapan, atau tata bahasa informal dari sampel sebagai ciri yang harus ditiru.

## Ciri utama yang harus dipertahankan

1. **Mulai dari jawaban inti setelah membersihkan basa-basi pembuka.** Pada sampel mentah, penulis kerap membuka dengan rujukan ke diri sendiri atau ke jawaban sebelumnya ("Kayaknya saya sudah jelaskan deh di 01...", "Sebenarnya, saya sudah tulis berkali kali sih...") sebelum masuk ke inti. Kalimat pembuka semacam itu adalah kebiasaan menulis cepat, bukan ciri gaya yang perlu dipertahankan. Untuk laporan, buang basa-basi ini dan langsung mendefinisikan objek, menyatakan masalah, atau mengambil posisi sejak kalimat pertama. Hindari pembukaan umum mengenai perkembangan teknologi.
2. **Bergerak dari gambaran besar ke mekanisme.** Berikan ringkasan satu atau dua kalimat, kemudian uraikan komponen, alur, dan pengecualian yang relevan.
3. **Gunakan hubungan sebab-akibat yang konkret.** Jelaskan kondisi, pengamatan, penyebab, tindakan, dan akibatnya. Jangan mengganti rangkaian ini dengan klaim umum seperti “meningkatkan efisiensi”.
4. **Jelaskan masalah teknis secara kronologis.** Pola alami penulis adalah asumsi awal, gejala yang ditemukan, pemeriksaan, kesadaran atas penyebab, perbaikan, lalu hasil atau pelajaran.
5. **Pertahankan contoh nyata.** Gunakan nama fitur, berkas, layanan, galat, atau skenario yang benar-benar dialami apabila tidak melanggar kerahasiaan.
6. **Nyatakan batas pengetahuan.** Apabila data, risiko, alasan, atau hasil tidak diketahui, tandai sebagai hal yang belum diketahui atau perlu diverifikasi. Jangan mengisi celah dengan kalimat yang terdengar meyakinkan.
7. **Ambil posisi dengan alasan praktis.** Argumentasi penulis cenderung menilai solusi berdasarkan kebutuhan perusahaan, alur kerja pengembang, keterbatasan sistem lama, dan konsekuensi implementasi.
8. **Gunakan istilah teknis yang alami.** Pertahankan istilah Inggris yang sudah familier bagi pengembang, seperti *container*, *deployment*, *feedback loop*, *endpoint*, *preview*, dan *default template*, daripada memaksakan terjemahan Indonesia yang tidak lazim. Ikuti aturan cetak miring dalam `AGENTS.md`.
9. **Gunakan pengulangan secara terbatas.** Gagasan utama boleh ditegaskan kembali apabila konteks atau konsekuensinya berubah, tetapi jangan mengulang klaim yang sama tanpa menambah informasi.
10. **Kenali pola asosiatif dan koreksi-diri penulis, lalu luruskan urutannya saat menulis laporan.** Draf mentah sering melompat antartopik dan kembali lagi sambil mengoreksi diri ("Oh sama soal integrasi, saya dari awal langsung kebayangnya...", lompatan antara pembahasan deployment, rencana fitur, dan diskusi mentor dalam sampel refleksi). Gunakan urutan kronologis/sebab-akibat yang sama seperti disebutkan pada butir 3--4, tetapi jangan biarkan lompatan asosiatif itu masuk ke naskah akhir; rapikan urutannya tanpa menghapus substansi koreksi-diri yang faktual.

## Bentuk paragraf yang disarankan

Untuk penjelasan sistem:

1. Nyatakan fungsi utama sistem.
2. Jelaskan alasan sistem diperlukan.
3. Uraikan komponen atau alur penggunaan.
4. Berikan satu contoh konkret.
5. Tutup dengan batasan atau pengecualian yang relevan.

Untuk persoalan teknis:

1. Nyatakan konteks dan asumsi awal.
2. Sebutkan gejala yang dapat diamati.
3. Jelaskan pemeriksaan dan temuan secara berurutan.
4. Nyatakan akar masalah tanpa mendramatisasi.
5. Jelaskan perubahan yang dilakukan dan cara memverifikasinya.
6. Catat konsekuensi atau pelajaran yang benar-benar diperoleh.

Untuk argumentasi:

1. Ambil posisi sejak awal.
2. Jelaskan manfaat utama secara konkret.
3. Akui keterbatasan yang benar-benar ada.
4. Gunakan contoh proyek untuk menguji posisi tersebut.
5. Tegaskan keputusan berdasarkan konteks, bukan klaim universal.

## Normalisasi untuk laporan akademik

Pertahankan cara berpikir penulis, tetapi lakukan normalisasi berikut:

- gunakan sudut pandang orang ketiga dengan konstruksi objektif, pasif, atau berbasis peran; jangan gunakan `aku`, `saya`, atau `kami` dalam naskah laporan;
- hapus sapaan, reaksi percakapan, dan pengisi seperti `bro`, `dong`, `sih`, `kan`, `yaudah`, `wow`, dan `gatau`;
- ubah pertanyaan retoris menjadi pernyataan langsung;
- perbaiki ejaan, kapitalisasi, dan bentuk kata tanpa mengubah isi;
- pecah rangkaian kalimat yang terlalu panjang menjadi kalimat sebab-akibat yang mudah dilacak;
- kurangi pengulangan setelah gagasan utama sudah memiliki bukti atau contoh;
- hindari pergantian bahasa pada satu klausa jika istilah teknis saja yang perlu dipertahankan dalam bahasa Inggris;
- jangan membuat narasi lebih rapi daripada faktanya: ketidakpastian dan pendekatan yang gagal tetap dicatat apabila substantif.

## Hal yang bukan bagian dari gaya

Jangan meniru:

- salah ketik dan ejaan fonetis;
- angka sebagai pengganti pengulangan kata;
- kapitalisasi yang tidak konsisten;
- campuran bahasa Inggris untuk kata nonteknis;
- komentar mengenai jumlah kata atau instruksi latihan;
- peralihan antara `aku` dan `saya`;
- klaim spontan yang belum memiliki bukti.

## Pemeriksaan sebelum menerima draf

Sebuah draf dianggap sesuai dengan gaya penulis apabila:

- pokok persoalan muncul pada awal bagian;
- setiap klaim manfaat memiliki mekanisme atau contoh;
- uraian teknis mengikuti urutan yang dapat dilacak;
- keterbatasan dan ketidakpastian tidak disembunyikan;
- istilah terasa seperti bahasa pengembang yang wajar, bukan hasil terjemahan harfiah;
- kalimat tetap formal tanpa kehilangan nada langsung dan praktis; dan
- tidak ada basa-basi akademik atau kesimpulan generik.
