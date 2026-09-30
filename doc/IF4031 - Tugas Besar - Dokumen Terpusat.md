# 

# 

# 

# **Tugas Besar** 

# **IF4031**

# **Arsitektur Aplikasi Terdistribusi**

## **Waktu Mulai**

## Rabu, 16 September 2026, 17:00 WIB

## 

## **Waktu Akhir**

## Jumat, 20 November 2026, 23.59 WIB

# ***Changelog***

Belum ada.

# 

# **Pendahuluan**

**Astolfo** sudah tiga tahun bertugas di ruang kendali Pusdalops, tetapi malam itu tetap saja terasa berbeda. Tiga layar di depannya menyala bergantian, satu menampilkan peta seismik dari BMKG, satu lagi grafik ketinggian permukaan laut, dan yang terakhir baru saja menerima notifikasi dari rekan sejawatnya di PVMBG: aktivitas anak gunung Prikitiw meningkat, ada indikasi longsoran di tubuh gunung. Yang membuat **Astolfo** gelisah adalah jeda di antara data itu. BMKG memantau gempa dan pasang laut lewat sistemnya sendiri, PVMBG memantau gunung api lewat sistemnya sendiri, dan tak satupun dari keduanya berkomunikasi dengan satu sama lain secara langsung. Ketika akhirnya tsunami kecil menerjang pesisir Selat **JAWA** tanpa didahului gempa besar, **Astolfo** baru sadar bahwa sistem yang ada itu dirancang untuk mendeteksi ancaman yang sudah dikenal, bukan untuk menyambungkan sinyal-sinyal lemah dari dua institusi yang berbeda otoritas, berbeda instrumen, dan berbeda bahasa data.

Beberapa bulan sebelumnya, cerita serupa terjadi di sisi lain Tamriel (negara). Titik api mulai bermunculan di lahan gambut Skypiea, terdeteksi oleh satelit yang datanya diproses instansi kehutanan, sementara BMKG mengeluarkan peringatan kualitas udara dari jalur yang sama sekali berbeda. BNPB, yang seharusnya menjadi simpul koordinasi, justru menerima dua laporan yang saling tidak sinkron. Satu bilang kebakaran masih kecil, satu bilang asap sudah menutupi tiga kabupaten. **Astolfo** ingat betul betapa timnya menghabiskan waktu bukan untuk memadamkan api, tapi untuk sekadar menyamakan angka.

Dan ketika gempa mengguncang salah satu wilayah di Tamriel bulan lalu, pola yang sama terulang lagi. Peringatan dini datang cepat dari BMKG, tetapi permintaan bantuan dari lapangan seperti rumah sakit, posko pengungsian, tim SAR, dsb. mengalir lewat saluran yang tak saling kenal satu sama lain. Setiap lembaga punya sistemnya sendiri, dibangun dengan asumsi sendiri, berjalan dengan kecepatan sendiri. Semuanya benar secara teknis. Tidak ada yang salah dalam kodenya. Tapi begitu beberapa sistem otonom itu harus bekerja bersama dalam satu peristiwa yang sama, masalahnya baru terlihat, siapa yang harus menunggu siapa, siapa yang boleh tahu apa, dan apa yang terjadi kalau salah satu di antaranya diam-diam berhenti merespons.

**Astolfo** tahu ini soal bagaimana banyak sistem yang otonom, yang dikembangkan oleh tim berbeda dengan keputusan desain berbeda, tetap bisa saling bertukar informasi secara benar, tetap tersedia saat salah satu bagian melemah, dan tetap bisa menampung lonjakan permintaan ketika krisis sedang berada di titik terpanas.

![][image1]

# **Teori Singkat**

Secara umum, aplikasi terdistribusi adalah kumpulan komponen independen yang saling berkomunikasi lewat jaringan untuk mencapai satu tujuan bersama, sambil berusaha tampil sebagai satu sistem yang koheren di mata penggunanya. Konsekuensinya. begitu sebuah sistem tersebar lintas proses dan mesin, sejumlah asumsi yang biasa dipegang dalam pemrograman satu mesin langsung runtuh. Tidak ada lagi memori bersama yang bisa diandalkan begitu saja, tidak ada jam global yang menjamin urutan kejadian sama persis dari sudut pandang semua pihak, dan yang paling penting; setiap komponen bisa gagal secara independen tanpa memberi tahu komponen lain.

Empat karakteristik inilah yang menjadi akar dari hampir semua tantangan yang akan dibahas sepanjang mata kuliah ini yaitu: *concurrency*, kegagalan parsial, heterogenitas (setiap layanan boleh dibangun dengan teknologi dan asumsi yang berbeda), dan skala.

Tugas besar ini dibagi menjadi **dua milestone** yang mengikuti urutan tantangan tersebut. Pada milestone pertama, *Sistem Terdistribusi Dasar*, fokusnya adalah menyusun fondasi agar beberapa layanan otonom (BMKG, PVMBG, dan BNPB) bisa saling berkomunikasi lewat antarmuka yang jelas, menangani banyak permintaan tanpa saling memblokir, dan tetap berjalan wajar walau salah satu di antaranya melambat. Pada milestone kedua, *Sistem Terdistribusi Skalabel*, sistem itu diuji dalam skenario yang lebih ekstrem. Bagaimana ia tetap bertahan ketika dijalankan di banyak *instance* sekaligus, bagaimana ketika volume data melonjak drastis, dan bagaimana ketika beberapa bagian sistem gagal bersamaan tanpa memicu kegagalan berantai ke seluruh sistem.

# **Deskripsi Tugas**

Kalian akan merancang dan mengimplementasikan sebuah arsitektur aplikasi terdistribusi untuk domain penanggulangan bencana, serta mengimplementasikan *proof-of-concept* (PoC) terhadap bagian yang dianggap paling kritis pada setiap milestone. Perancangan dan implementasi dilakukan secara bertahap mengikuti **dua milestone** yang telah dijelaskan sebelumnya — *Sistem Terdistribusi Dasar* dan *Sistem Terdistribusi Skalabel* — dengan tantangan yang meningkat sejalan dengan materi kuliah pada tiap tahap.

Aplikasi yang akan kalian rancang adalah sebuah platform yang menjembatani pertukaran informasi dan koordinasi respons antar lembaga-lembaga otonom yang menangani kebencanaan di Tamriel, yakni BMKG (pemantauan gempa, cuaca, dan potensi tsunami), PVMBG/Badan Geologi (pemantauan aktivitas gunung api), serta BNPB (koordinasi respons dan pengambilan keputusan lintas wilayah), beserta pihak-pihak pendukung lain seperti instansi pemadam kebakaran hutan, tim SAR, pemerintah daerah, dan warga sipil sebagai pelapor kondisi lapangan maupun penerima informasi.

Skala persoalan ini bersifat nasional: jenis bencana yang harus ditangani beragam (vulkanik, seismik, kebakaran hutan, hidrometeorologi), setiap jenis dipantau oleh instansi dengan instrumen dan format data yang berbeda, dan beban sistem bisa melonjak drastis dalam hitungan menit begitu status berubah dari normal ke siaga/darurat. Kombinasi faktor-faktor  ini (banyak instansi otonom, format data heterogen, kebutuhan ketersediaan tinggi, serta beban yang sangat fluktuatif) membuat arsitektur terdistribusi menjadi suatu keharusan, bukan sekadar pilihan desain.

Berbeda dari format tugas besar pada umumnya, kebutuhan fungsional dan non-fungsional pada tugas besar ini tidak dijabarkan langsung di bagian ini. Seluruh tantangan yang perlu kalian jawab, baik dari sisi fungsionalitas maupun kualitas sistem, diberikan secara terpisah dalam bentuk **problem set** pada dokumen spesifikasi masing-masing milestone.

Tautan dokumen spesifikasi serta tenggat pengumpulan setiap milestone adalah sebagai berikut:

| Tahapan | Laman Spesifikasi | Deadline |
| :---- | :---- | :---- |
| Sistem Terdistribusi Dasar | [IF4031 - Tugas Besar - M1](https://docs.google.com/document/d/1QOSgL0U4eBN-16b3Hkr7ohzDLYPmPC0tlI-CoFZSRm8/edit?tab=t.0) | Jumat, 9 Oktober 2026 Pukul 23.59 WIB  |
| Sistem Terdistribusi Skalabel | \[TBA\] | Minggu, 20 November 2026  Pukul 23.59 WIB |

Perlu ditekankan bahwa dua milestone ini **bersifat berkelanjutan**: sistem yang dibangun pada milestone Sistem Terdistribusi Dasar menjadi fondasi yang akan diskalakan dan diperluas pada milestone Sistem Terdistribusi *Scalable*. Asumsi dan keputusan desain yang diambil pada milestone pertama akan terus terbawa dan diuji kembali pada milestone kedua.

Selain itu juga terdapat beberapa ketentuan umum yang berlaku untuk pengerjaan semua milestone, yaitu:

1. Deadline pengumpulan bersifat **HARD DEADLINE**.

2. Diharuskan membuat laporan untuk masing-masing milestone-nya yang ketentuannya dijelaskan pada setiap dokumen spesifikasi.

3. Penilaian akan dilakukan per pengumpulan milestone. Pastikan kode yang dikumpulkan pada setiap milestone-nya bisa dijalankan dan juga berikan README yang memuat cara menjalankannya dengan lengkap dan jelas.

4. Komponen penilaian akan dijelaskan pada setiap dokumen spesifikasi.

5. Apabila diperlukan, mahasiswa juga diperbolehkan untuk mengadakan asistensi. Untuk teknis asistensinya diserahkan kepada mahasiswa sendiri untuk berkoordinasi langsung dengan asisten.

# **Teknis Pengerjaan**

Berikut merupakan teknis pengerjaan tugas besar:

1. Tugas besar dikerjakan secara berkelompok, dengan masing-masing kelompok terdiri atas 3 anggota. Kelompok yang terdiri atas 4 anggota dapat dibentuk hanya apabila jumlah anggota kelas tidak habis dibagi 3, dan seluruh kelompok lain di sheets sudah memiliki 3 anggota.

   1. Isi kelompok pada [sheets pemilihan kelompok](https://docs.google.com/spreadsheets/d/1gtJHySK0V-1Pkw-QJiVhDByd54c_OwvTwKYfbgmosLY/edit?gid=1482421942#gid=1482421942). Pastikan kelompok sudah tertulis sebelum **Minggu, 19 September 2026, pukul 23.59.**

   2. Peserta kuliah yang tidak tercantum pada sheets pada akhir masa pengisian kelompok akan dikelompokkan satu sama lain, atau disebarkan ke kelompok-kelompok lain yang sudah terdaftar.

   3. Peserta *sit-in* dipersilakan mengisi *sheets* **setelah** periode URS berakhir.

   4. Setiap anggota kelompok wajib berkontribusi ke tugas besar.

2. Diperbolehkan untuk menggunakan LLM untuk membantu menuliskan dokumen rancangan, dengan syarat penggunaannya dideklarasikan di laporan. Diperbolehkan juga untuk menggunakan LLM untuk melakukan implementasi, selama kode yang dihasilkan dipahami, dan penggunaan LLM diakui dan dituliskan pada kode.

   1. Jika diketahui terdapat bagian laporan atau kode yang sama dengan hasil pekerjaan lain maupun repository di internet, maka akan dianggap melakukan kecurangan. Alasan menggunakan bantuan LLM akan diabaikan.

   2. Baca [aturan penggunaan AI yang diperbolehkan dalam lingkup ITB.](https://drive.google.com/file/d/1IuoMfSx4PZAU-M-iwY8cE0ZWcp-0WQoS/view?usp=drive_link)

   3. Semua kelompok **wajib** melampirkan melampirkan formulir berikut pada laporan: [Form Penggunaan AI.pdf](https://drive.google.com/file/d/1QjMlSLzxh5uWctNiTBJXnSMcxlknfK5C/view?usp=sharing)

3. Segala kecurangan baik sengaja dan tidak disengaja akan ditindaklanjuti, yang dapat berakibat sanksi akademik ke setiap pihak yang terlibat.

# **QnA**

Apabila ada yang ingin ditanyakan, dapat merujuk ke [IF4031 - QnA Tugas Besar AAT 2026](https://docs.google.com/spreadsheets/d/1twlPGtv6hU_0F60tNZ8fr78eNmEi9hNnxa7m2djfIJg/edit?gid=0#gid=0). Pastikan pertanyaan yang ditanyakan **tidak berulang**. Asisten memiliki hak untuk **tidak** menjawab bila pertanyaan tersebut redundan.

# 

# **Pesan Asisten**

![][image2]

“Welcome to AAT”  
\-Setsuna/Riko

![][image3]

“Welcome, ‘O Astolfo lover”  
\-Bimo

![][image4]

“glhf”  
\-reletz/Nau
