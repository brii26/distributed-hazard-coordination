# **PLATFORM KOORDINASI KEBENCANAAN**

**Laporan Tugas Besar Milestone 1**  
Disusun untuk memenuhi tugas mata kuliah Arsitektur Aplikasi Terdistribusi pada Semester 7 (tujuh)  
Tahun Akademik 2026/2027

![][image1]

Disusun Oleh **Kelompok Ingress**:  
Shanice Feodora Tjahjono	13523097  
Brian Ricardo Tamin		13523126  
Nathanael Rachmat		13523142

**PROGRAM STUDI TEKNIK INFORMATIKA**  
**SEKOLAH TEKNIK ELEKTRO DAN INFORMATIKA**  
**INSTITUT TEKNOLOGI BANDUNG**  
**BANDUNG**  
**2026**

# **DAFTAR ISI** {#daftar-isi}

[**DAFTAR ISI	2**](#daftar-isi)

[**TABEL KONTRIBUSI	6**](#tabel-kontribusi)

[**BAB I**](#bab-i-deskripsi-umum-sistem)  
[**Deskripsi Umum Sistem	7**](#bab-i-deskripsi-umum-sistem)

[1.1 Gambaran Ringkas Sistem	7](#1.1-gambaran-ringkas-sistem)

[1.1.1 Sub Subbab1	7](#1.1.1-sub-subbab1)

[1.1.2 Sub Subbab1	7](#1.1.2-sub-subbab1)

[1.2 Diagram arsitektur sistem	7](#1.2-diagram-arsitektur-sistem)

[1.2.1 Sub Subbab1	7](#1.2.1-sub-subbab1)

[1.2.2 Sub Subbab1	7](#1.2.2-sub-subbab1)

[**BAB II**](#bab-ii-diagram-arsitektur-sistem)  
[**Diagram Arsitektur Sistem	8**](#bab-ii-diagram-arsitektur-sistem)

[2.1 Diagram Arsitektur	8](#2.1-diagram-arsitektur)

[2.1.1 Sub Subbab1	8](#2.1.1-sub-subbab1)

[2.1.2 Sub Subbab1	8](#2.1.2-sub-subbab1)

[2.2 Penjelasan Naratif Diagram	8](#2.2-penjelasan-naratif-diagram)

[2.2.1 Sub Subbab1	8](#2.2.1-sub-subbab1)

[2.2.2 Sub Subbab1	8](#2.2.2-sub-subbab1)

[**BAB III**](#bab-iii-daftar-teknologi-terpilih)  
[**Daftar Teknologi Terpilih	9**](#bab-iii-daftar-teknologi-terpilih)

[3.1 Subbab	9](#3.1-subbab)

[3.1.1 Sub Subbab1	9](#3.1.1-sub-subbab1)

[3.1.2 Sub Subbab1	9](#3.1.2-sub-subbab1)

[3.2 Subbab	9](#3.2-subbab)

[3.2.1 Sub Subbab1	9](#3.2.1-sub-subbab1)

[3.2.2 Sub Subbab1	9](#3.2.2-sub-subbab1)

[**BAB IV**](#bab-iv-asumsi)  
[**Asumsi	10**](#bab-iv-asumsi)

[4.1 Subbab	10](#4.1-subbab)

[4.1.1 Sub Subbab1	10](#4.1.1-sub-subbab1)

[4.1.2 Sub Subbab1	10](#4.1.2-sub-subbab1)

[4.2 Subbab	10](#4.2-subbab)

[4.2.1 Sub Subbab1	10](#4.2.1-sub-subbab1)

[4.2.2 Sub Subbab1	10](#4.2.2-sub-subbab1)

[**BAB V**](#bab-v-petunjuk-menjalankan-sistem)  
[**Petunjuk Menjalankan Sistem	11**](#bab-v-petunjuk-menjalankan-sistem)

[5.1 Subbab	11](#5.1-subbab)

[5.1.1 Sub Subbab1	11](#5.1.1-sub-subbab1)

[5.1.2 Sub Subbab1	11](#5.1.2-sub-subbab1)

[5.2 Subbab	11](#5.2-subbab)

[5.2.1 Sub Subbab1	11](#5.2.1-sub-subbab1)

[5.2.2 Sub Subbab1	11](#5.2.2-sub-subbab1)

[**BAB VI**](#bab-vi-problem-1---arsitektur-&-interoperabilitas-data)  
[**Problem 1 \- Arsitektur & Interoperabilitas Data	12**](#bab-vi-problem-1---arsitektur-&-interoperabilitas-data)

[6.1 Rancangan Solusi	12](#6.1-rancangan-solusi)

[6.2 Alasan Keputusan	13](#6.2-alasan-keputusan)

[6.3 Batasan Yang Disadari	14](#6.3-batasan-yang-disadari)

[6.4 Bukti Penyelesaian	14](#6.4-bukti-penyelesaian)

[6.4.1 Aggregator Menghasilkan HazardEvent	14](#6.4.1-aggregator-menghasilkan-hazardevent)

[6.4.2 Picu Perubahan Skema PVMBG	14](#6.4.2-picu-perubahan-skema-pvmbg)

[6.4.3 Aggregator Tetap Menghasilkan HazardEvent	14](#6.4.3-aggregator-tetap-menghasilkan-hazardevent)

[6.5 Kebijakan Penanganan Field Tak Dikenal	14](#6.5-kebijakan-penanganan-field-tak-dikenal)

[6.6 Mekanisme yang Dipilih	14](#6.6-mekanisme-yang-dipilih)

[**BAB VII**](#bab-vii-problem-2---concurrency,-availability,-&-graceful-degradation)  
[**Problem 2 \- Concurrency, Availability, & Graceful Degradation	15**](#bab-vii-problem-2---concurrency,-availability,-&-graceful-degradation)

[7.1 Rancangan Solusi	15](#7.1-rancangan-solusi)

[7.1.1 Pemisahan Jalur Ingest dan Jalur Pelayanan	15](#7.1.1-pemisahan-jalur-ingest-dan-jalur-pelayanan)

[7.1.2 Poller Independen per Sumber	15](#7.1.2-poller-independen-per-sumber)

[7.1.3 Pembatasan Konkurensi	16](#7.1.3-pembatasan-konkurensi)

[7.1.4 Status Kebaruan Data dan Degradasi	16](#7.1.4-status-kebaruan-data-dan-degradasi)

[7.1.5 Parameter Konfigurasi	17](#7.1.5-parameter-konfigurasi)

[7.2 Alasan Keputusan	17](#7.2-alasan-keputusan)

[7.2.1 Pemisahan Jalur Ingest dan Jalur Akses	17](#7.2.1-pemisahan-jalur-ingest-dan-jalur-akses)

[7.2.2 Model Konkurensi Asynchronous Non-Blocking I/O	18](#7.2.2-model-konkurensi-asynchronous-non-blocking-i/o)

[7.2.3 Poller Independen dengan Single In-Flight	18](#7.2.3-poller-independen-dengan-single-in-flight)

[7.2.4 Load Shedding pada Global Concurrency Cap	19](#7.2.4-load-shedding-pada-global-concurrency-cap)

[7.2.5 Rate Limiting Token Bucket per Client	19](#7.2.5-rate-limiting-token-bucket-per-client)

[7.2.6 Circuit Breaker dan Respons Stale dengan Penanda	19](#7.2.6-circuit-breaker-dan-respons-stale-dengan-penanda)

[7.3 Batasan Yang Disadari	20](#7.3-batasan-yang-disadari)

[7.3.1 Data Bersifat Eventual Consistent	20](#7.3.1-data-bersifat-eventual-consistent)

[7.3.2 Deteksi Outage Tidak Instan	20](#7.3.2-deteksi-outage-tidak-instan)

[7.3.3 Sumber yang Sangat Lambat Diperlakukan Sama dengan Sumber yang Mati	20](#7.3.3-sumber-yang-sangat-lambat-diperlakukan-sama-dengan-sumber-yang-mati)

[7.3.4 Load shedding tidak memprioritaskan client	20](#7.3.4-load-shedding-tidak-memprioritaskan-client)

[7.3.5 Satu instance per service dan state in-memory	21](#7.3.5-satu-instance-per-service-dan-state-in-memory)

[7.3.6 Aggregator dan Canonical Store Merupakan Dependensi Jalur Akses	21](#7.3.6-aggregator-dan-canonical-store-merupakan-dependensi-jalur-akses)

[7.3.7 Hasil load testing bergantung pada lingkungan pengujian	21](#7.3.7-hasil-load-testing-bergantung-pada-lingkungan-pengujian)

[7.4 Bukti Penyelesaian	21](#7.4-bukti-penyelesaian)

[7.4.1 Uji p95 BMKG-only saat PVMBG delay 3 detik	21](#7.4.1-uji-p95-bmkg-only-saat-pvmbg-delay-3-detik)

[7.4.2 Load Testing (≥50 koneksi, 60 detik)	21](#7.4.2-load-testing-\(≥50-koneksi,-60-detik\))

[7.4.3 Demo Outage PVMBG	21](#7.4.3-demo-outage-pvmbg)

[7.5 Model Konkurensi dan Mitigasi Kegagalan	21](#7.5-model-konkurensi-dan-mitigasi-kegagalan)

[7.5.1 Model Konkurensi	21](#7.5.1-model-konkurensi)

[7.5.2 Mitigasi Head-of-Line Blocking	21](#7.5.2-mitigasi-head-of-line-blocking)

[7.5.3 Mitigasi Resource Exhaustion	22](#7.5.3-mitigasi-resource-exhaustion)

[7.5.4 Mitigasi Ketiadaan Degradasi Terkendali	22](#7.5.4-mitigasi-ketiadaan-degradasi-terkendali)

[7.6 Konfigurasi dan Tooling Load Testing	22](#7.6-konfigurasi-dan-tooling-load-testing)

[**BAB VIII**](#bab-viii-problem-3---service-to-service-authentication-&-trust)  
[**Problem 3 \- Service-to-Service Authentication & Trust	23**](#bab-viii-problem-3---service-to-service-authentication-&-trust)

[8.1 Rancangan Solusi	23](#8.1-rancangan-solusi)

[8.1.1 Otentikasi dan Otorisasi antar Servis	23](#8.1.1-otentikasi-dan-otorisasi-antar-servis)

[8.1.2 Otentikasi dan Otorisasi Client ke Service	25](#8.1.2-otentikasi-dan-otorisasi-client-ke-service)

[8.2 Alasan Keputusan	26](#8.2-alasan-keputusan)

[8.2.1 Mekanisme Antar-Service: OAuth 2.0 vs SAML vs OpenID Connect vs Shared Key	26](#8.2.1-mekanisme-antar-service:-oauth-2.0-vs-saml-vs-openid-connect-vs-shared-key)

[8.2.2 Format Access Token PVMBG: Opaque Token vs JWT	27](#8.2.2-format-access-token-pvmbg:-opaque-token-vs-jwt)

[8.2.3 Autentikasi Client ke BNPB: JWT vs Session-Based	27](#8.2.3-autentikasi-client-ke-bnpb:-jwt-vs-session-based)

[8.2.4 Mekanisme Tanda Tangan JWT: Hashed-based Message Authentication Code vs Digital Signature	28](#8.2.4-mekanisme-tanda-tangan-jwt:-hashed-based-message-authentication-code-vs-digital-signature)

[8.2.5 Mekanisme Refresh: Client-Side Refresh vs Transparent Refresh	28](#8.2.5-mekanisme-refresh:-client-side-refresh-vs-transparent-refresh)

[8.3 Batasan Yang Disadari	29](#8.3-batasan-yang-disadari)

[8.4 Bukti Penyelesaian	29](#8.4-bukti-penyelesaian)

[8.4.1 Kredensial BMKG Ditolak di PVMBG (dan sebaliknya)	29](#8.4.1-kredensial-bmkg-ditolak-di-pvmbg-\(dan-sebaliknya\))

[8.4.2 Scope Media Ditolak Akses Field Mentah	29](#8.4.2-scope-media-ditolak-akses-field-mentah)

[8.4.3 Token Expiry \+ Refresh Flow Tim Lapangan	29](#8.4.3-token-expiry-+-refresh-flow-tim-lapangan)

[8.4.4 Tidak Ada Hardcoded Credential	29](#8.4.4-tidak-ada-hardcoded-credential)

[8.5 Mekanisme yang dipakai beserta alasan mengapa mekanisme tersebut tidak membuka celah baru.	29](#8.5-mekanisme-yang-dipakai-beserta-alasan-mengapa-mekanisme-tersebut-tidak-membuka-celah-baru.)

[**BAB IX**](#bab-ix-problem-4---arsitektur-microservice-&-flexible-storage)  
 [**Problem 4 \- Arsitektur Microservice & Flexible Storage	30**](#bab-ix-problem-4---arsitektur-microservice-&-flexible-storage)

[9.1 Rancangan Solusi	30](#9.1-rancangan-solusi)

[9.2 Alasan Keputusan	31](#9.2-alasan-keputusan)

[9.2.1 Batas Service	31](#9.2.1-batas-service)

[9.3 Batasan Yang Disadari	31](#9.3-batasan-yang-disadari)

[9.3.1 Restart Auth/Token Service berdampak pada login dan refresh	31](#9.3.1-restart-auth/token-service-berdampak-pada-login-dan-refresh)

[9.3.2 Aggregator merupakan dependensi utama jalur data	31](#9.3.2-aggregator-merupakan-dependensi-utama-jalur-data)

[9.3.3 Canonical Store berjalan sebagai satu instance	32](#9.3.3-canonical-store-berjalan-sebagai-satu-instance)

[9.3.4 Fleksibilitas skema hanya berlaku untuk atribut sumber	32](#9.3.4-fleksibilitas-skema-hanya-berlaku-untuk-atribut-sumber)

[9.3.5 Isi attributes tidak divalidasi dan tidak di-index	32](#9.3.5-isi-attributes-tidak-divalidasi-dan-tidak-di-index)

[9.4 Bukti Penyelesaian	32](#9.4-bukti-penyelesaian)

[9.4.1 Restart Independen Container	32](#9.4.1-restart-independen-container)

[9.4.2 Record Sebelum/Sesudah Schema Change	32](#9.4.2-record-sebelum/sesudah-schema-change)

[9.4.3 Canonical Store Hanya Diakses Aggregator	32](#9.4.3-canonical-store-hanya-diakses-aggregator)

[9.5 Batas Service dan Alternatif Penyimpanan	32](#9.5-batas-service-dan-alternatif-penyimpanan)

[9.5.1 Alternatif Batasan Service	32](#9.5.1-alternatif-batasan-service)

[9.5.2 Alternatif Penyimpanan Canonical Store	33](#9.5.2-alternatif-penyimpanan-canonical-store)

[**BAB X**](#bab-x-problem-5---fan-out-event-distribution/pub-sub)  
[**Problem 5 \- Fan-out Event Distribution/Pub-sub	35**](#bab-x-problem-5---fan-out-event-distribution/pub-sub)

[10.1 Rancangan Solusi	35](#10.1-rancangan-solusi)

[10.2 Alasan Keputusan	36](#10.2-alasan-keputusan)

[10.2.1 Pemilihan Broker	36](#10.2.1-pemilihan-broker)

[10.2.2 Jenis Exchange	36](#10.2.2-jenis-exchange)

[10.2.3 Queue per Consumer	36](#10.2.3-queue-per-consumer)

[10.2.4 Kebijakan Buffer Producer	36](#10.2.4-kebijakan-buffer-producer)

[10.2.5 Manual Acknowledgement	36](#10.2.5-manual-acknowledgement)

[10.2.6 Kunci Deduplikasi	37](#10.2.6-kunci-deduplikasi)

[10.2.7 Penyimpanan Status Deduplikasi	37](#10.2.7-penyimpanan-status-deduplikasi)

[10.3 Batasan Yang Disadari	37](#10.3-batasan-yang-disadari)

[10.4 Bukti Penyelesaian	38](#10.4-bukti-penyelesaian)

[10.4.1 Publish ke Broker	38](#10.4.1-publish-ke-broker)

[10.4.2 Dua Consumer Independen	38](#10.4.2-dua-consumer-independen)

[10.4.3 Case Consumer Mati	38](#10.4.3-case-consumer-mati)

[10.4.4 Penambahan Consumer Baru Tanpa Ubah Producer	38](#10.4.4-penambahan-consumer-baru-tanpa-ubah-producer)

[10.5 Broker, Pola, dan Semantik Pengiriman	38](#10.5-broker,-pola,-dan-semantik-pengiriman)

[10.5.1 Broker dan Pola	38](#10.5.1-broker-dan-pola)

[10.5.2 Semantik Pengiriman	38](#10.5.2-semantik-pengiriman)

[10.5.3 Konsekuensi bagi Consumer	38](#10.5.3-konsekuensi-bagi-consumer)

[**Deklarasi Penggunaan LLM	39**](#deklarasi-penggunaan-llm)

[10.4.3 Subbab	39](#10.4.3-subbab)

[10.4.4 Subbab	39](#10.4.4-subbab)

[**REFERENSI	40**](#referensi)

# **TABEL KONTRIBUSI** {#tabel-kontribusi}

| NIM | Nama | Kontribusi |
| ----- | ----- | ----- |
| 13523097 | Shanice Feodora Tjahjono | Asd Asd |
| 13523126 | Brian Ricardo Tamin | helo helohelo |
| 13523142 | Nathanael Rachmat | Asd Asd |

### 

# **BAB I** **Deskripsi Umum Sistem** {#bab-i-deskripsi-umum-sistem}

## **1.1 Gambaran Ringkas Sistem** {#1.1-gambaran-ringkas-sistem}

### **1.1.1 Sub Subbab1** {#1.1.1-sub-subbab1}

\<placeholder\> 

### **1.1.2 Sub Subbab1** {#1.1.2-sub-subbab1}

\=\<placeholder\> 

## **1.2 Diagram arsitektur sistem** {#1.2-diagram-arsitektur-sistem}

### **1.2.1 Sub Subbab1** {#1.2.1-sub-subbab1}

\<placeholder\>

### **1.2.2 Sub Subbab1** {#1.2.2-sub-subbab1}

\<placeholder\>

# **BAB II** **Diagram Arsitektur Sistem** {#bab-ii-diagram-arsitektur-sistem}

## **2.1 Diagram Arsitektur** {#2.1-diagram-arsitektur}

### **2.1.1 Sub Subbab1** {#2.1.1-sub-subbab1}

\<placeholder\> 

### **2.1.2 Sub Subbab1** {#2.1.2-sub-subbab1}

\<placeholder\> 

## **2.2 Penjelasan Naratif Diagram** {#2.2-penjelasan-naratif-diagram}

### **2.2.1 Sub Subbab1** {#2.2.1-sub-subbab1}

\<placeholder\>

### **2.2.2 Sub Subbab1** {#2.2.2-sub-subbab1}

\<placeholder\>

# **BAB III** **Daftar Teknologi Terpilih** {#bab-iii-daftar-teknologi-terpilih}

## **3.1 Subbab** {#3.1-subbab}

### **3.1.1 Sub Subbab1** {#3.1.1-sub-subbab1}

\<placeholder\> 

### **3.1.2 Sub Subbab1** {#3.1.2-sub-subbab1}

\<placeholder\> 

## **3.2 Subbab** {#3.2-subbab}

### **3.2.1 Sub Subbab1** {#3.2.1-sub-subbab1}

\<placeholder\>

### **3.2.2 Sub Subbab1** {#3.2.2-sub-subbab1}

\<placeholder\>

# **BAB IV** **Asumsi** {#bab-iv-asumsi}

## **4.1 Subbab** {#4.1-subbab}

### **4.1.1 Sub Subbab1** {#4.1.1-sub-subbab1}

\<placeholder\> 

### **4.1.2 Sub Subbab1** {#4.1.2-sub-subbab1}

\<placeholder\> 

## **4.2 Subbab** {#4.2-subbab}

### **4.2.1 Sub Subbab1** {#4.2.1-sub-subbab1}

\<placeholder\>

### **4.2.2 Sub Subbab1** {#4.2.2-sub-subbab1}

\<placeholder\>

# **BAB V** **Petunjuk Menjalankan Sistem** {#bab-v-petunjuk-menjalankan-sistem}

## **5.1 Subbab** {#5.1-subbab}

### **5.1.1 Sub Subbab1** {#5.1.1-sub-subbab1}

\<placeholder\> 

### **5.1.2 Sub Subbab1** {#5.1.2-sub-subbab1}

\<placeholder\> 

## **5.2 Subbab** {#5.2-subbab}

### **5.2.1 Sub Subbab1** {#5.2.1-sub-subbab1}

\<placeholder\>

### **5.2.2 Sub Subbab1** {#5.2.2-sub-subbab1}

\<placeholder\>

# **BAB VI** **Problem 1 \- Arsitektur & Interoperabilitas Data** {#bab-vi-problem-1---arsitektur-&-interoperabilitas-data}

## **6.1 Rancangan Solusi** {#6.1-rancangan-solusi}

Menggunakan REST sebagai metode komunikasi antara BNPB dengan BMKG/PVMBG. Kontrak GET API BMKG akan terlihat berikut ini:  
GET /seismic-events?since=\<timestamp\>

| `{     "event_id": <string_value>,     "magnitude": <float_value>,     "depth_km": <float_value>,     "epicenter_lat": <float_value>,     "epicenter_lon": <float_value>,     "region_name": <string_value>,     "occurred_at": <datetime_value_in_string>,     "potential_tsunami": <boolean_value>, }` |
| :---- |

GET /tsunami-warnings?since=\<timestamp\>

| `{     "warning_id": <string_value>,     "related_event_id": <string_value>,     "threat_level": <enum_value_in_string>,     "affected_zones":     [         <string_value>,         <string_value>,     ],     "estimated_arrival": <date_time>, }` |
| :---- |

Sedangkan, Kontrak GET API PVMBG akan terlihat seperti berikut ini:  
GET /volcanic-reports?since=\<timestamp\>

| `{     "report_id": <string_value>,     "volcano_id": <string_value>,     "alert_level": <enum_value_in_string>,     "eruption_count_24h": <integer_value>,     "ash_column_height_m": <float_value>,     "reported_at": <datetime_value>, }` |
| :---- |

Kontrak API di atas merupakan yang terdefinisikan dalam BNPB dan bagaimana cara BNBP memetakan nilainya sesuai dengan [spesifikasi tugas besar](https://docs.google.com/document/d/1QOSgL0U4eBN-16b3Hkr7ohzDLYPmPC0tlI-CoFZSRm8/edit?tab=t.0#heading=h.ql4pp1hq9xtx). Untuk seluruh *field* yang belum didefinisikan tapi muncul dalam JSON yang dikembalikan, BNBP akan memetakan field baru tersebut ke dalam field *attributes*. Untuk seluruh *fields* yang sudah didefinisikan tapi tidak ada dalam JSON yang dikembalikan, field terkait bernilai *null*. Untuk seluruh field yang di-*rename*, mekanisme sebelumnya (field removal dan field addition) akan secara otomatis menangani masalah ini. Untuk tipe data yang diubah, field terkait akan dimasukkan ke *attribute field* dengan nama key yang sama. 

## **6.2 Alasan Keputusan** {#6.2-alasan-keputusan}

Pertama, [kebijakan penanganan](#6.5-kebijakan-penanganan-field-tak-dikenal) yang didefinisikan seperti demikian. Kedua, tertulis bahwa penambahan field baru oleh PVMBG adalah perubahan internal mereka sendiri, bukan urusan BNPB. Bagaimana cara PVMBG mengembalikan struktur JSON itu terserah PVMBG, tidak ada perjanjian yang dibuat antara BNPB dengan PVMBG. Dengan itu, BNPB harus bisa secara fleksibel support sudden schema change dari kedua BMKG dan PVMBG. Opsi lain seperti gRPC mendukung atribut yang fleksibel. Contohnya menggunakan gRPC, *message* dapat didefinisikan agar mensimulasikan JSON (Paani, 2023):

| message Person {    string name \= 1;    int32 age \= 2;    google.protobuf.Struct additionalInfo \= 3; } |
| :---- |

Atau menggunakan map string string dalam Thrift (roger, 2015):

| `struct QuerySetRecord {     ...     4:map<string,string> dataMap,     ... }` |
| :---- |

Sedangkan GraphQL tidak support dynamic fields. Pendefinisian skema *dynamic field* tersebut bisa dipakai jika PVMBG dan BMKG peduli terhadap urusan BNPB. Semisal pakai gRPC, ada dua pendekatan:

1. Tambah message yang support *dynamic field* dalam protobuf, sehingga field baru dimasukkan masing-masing oleh PVMBG dan BMKG ke dalam field dinamis tersebut sebelum dikembalikan ke BNPB. Pendekatan ini sama sekali tidak merepresentasikan yang ditulis oleh spek. PVMBG tidak peduli bagaimana cara skema mereka di-*parse* oleh server luar. Pasti mereka akan prioritaskan kebutuhan internal mereka (tanpa menggunakan field dinamis sama sekali dalam skema protobuf).  
2. Skema dari PVMBG atau BMKG diubah secara internal. Pendekatan ini tidak mendukung kebijakan yang sudah didefinisikan, karena walaupun BNPB masih bisa parse tanpa error, field yang baru ditambahkan itu tidak akan berhasil di-*parse* (skema internal BNPB belum berubah, sebelum field baru ditambahkan). Belum lagi kalau urutan skema dalam sebuah message berbeda.

Untuk thrift, pendekatannya sama seperti penjelasan di atas. Dengan demikian, pendekatan REST JSON yang fleksibel merupakan pilihan yang tepat untuk *problem set* 1\. REST JSON tidak memerlukan field tersendiri khusus field dinamis, dan masih dapat di-*parse* secara utuh oleh  BNPB-nya.

## **6.3 Batasan Yang Disadari** {#6.3-batasan-yang-disadari}

Pendekatan ini mendukung berbagai macam perubahan yang terjadi dalam skema API nya:

1. Penambahan field baru: dimasukkan ke dalam field attribute.  
2. Pengurangan field: nilai field terkait menjadi null.  
3. Perubahan nama field: dimasukkan ke dalam field attribute, dan field terkait nama sebelumnya bernilai *null*.  
4. Perubahan tipe data: dimasukkan ke dalam field attribute dengan key yang sama.

## **6.4 Bukti Penyelesaian** {#6.4-bukti-penyelesaian}

### **6.4.1 Aggregator Menghasilkan HazardEvent** {#6.4.1-aggregator-menghasilkan-hazardevent}

\<placeholder\> 

### **6.4.2 Picu Perubahan Skema PVMBG** {#6.4.2-picu-perubahan-skema-pvmbg}

\<placeholder\> 

### **6.4.3 Aggregator Tetap Menghasilkan HazardEvent** {#6.4.3-aggregator-tetap-menghasilkan-hazardevent}

\<placeholder\> 

## **6.5 Kebijakan Penanganan Field Tak Dikenal** {#6.5-kebijakan-penanganan-field-tak-dikenal}

Semua perubahan data yang tidak didefinisikan dalam code dimasukkan ke attributes.

## **6.6 Mekanisme yang Dipilih** {#6.6-mekanisme-yang-dipilih}

Mekanisme yang dipilih adalah dynamic field mapping. Seperti yang disebutkan [di atas](#6.3-batasan-yang-disadari), jenis perubahan yang didukung berupa *field addition*, *removal*, *rename*, dan *data type change*.

# **BAB VII** **Problem 2 \- Concurrency, Availability, & Graceful Degradation** {#bab-vii-problem-2---concurrency,-availability,-&-graceful-degradation}

## **7.1 Rancangan Solusi** {#7.1-rancangan-solusi}

### **7.1.1 Pemisahan Jalur Ingest dan Jalur Pelayanan** {#7.1.1-pemisahan-jalur-ingest-dan-jalur-pelayanan}

Sistem memisahkan dua jalur yang sebelumnya menyatu dalam satu alur: 

* Jalur ingest: Aggregator melakukan polling ke BMKG dan PVMBG di belakang, memetakan hasilnya ke HazardEvent, lalu menyimpannya ke Canonical Store.   
* Jalur pelayanan: permintaan client diterima Client-Facing API, diteruskan ke read API milik Aggregator, dan dijawab dari Canonical Store.

Dengan pemisahan ini, permintaan client tidak pernah menunggu respons BMKG maupun PVMBG secara langsung. Latensi PVMBG (500 ms-3 detik) hanya berdampak pada seberapa cepat data vulkanik baru masuk ke store, bukan pada latensi permintaan client.

Endpoint penyajian dipisah berdasarkan jenis bahaya: 

* GET /hazards?type=SEISMIC \-\> hanya membaca data bersumber BMKG   
* GET /hazards?type=VOLCANIC \-\> hanya membaca data bersumber PVMBG 

Permintaan "BMKG-only" pada kriteria penyelesaian merujuk ke endpoint seismik, yang sama sekali tidak menyentuh status maupun data PVMBG.

### **7.1.2 Poller Independen per Sumber** {#7.1.2-poller-independen-per-sumber}

Setiap sumber memiliki poller sendiri yang berjalan sebagai task asinkron terpisah di dalam Agregator:

* Poller BMKG dan poller PVMBG memiliki loop, interval, dan timeout masing-masing, sehingga poller BMKG tidak pernah menunggu poller PVMBG.  
* Setiap poller hanya menjalankan satu panggilan dalam satu waktu (single in-flight), panggilan berikutnya baru dimulai setelah panggilan sebelumnya selesai atau terkena timeout. Waktu tunggu sebelum panggilan berikutnya dihitung sebagai interval polling dikurangi durasi panggilan sebelumnya. Jika durasi panggilan melebihi interval, panggilan berikutnya langsung dijalankan tanpa menunggu. Dengan demikian, jarak antar panggilan tetap sebesar interval saat sumber normal, tidak melebihi durasi timeout saat sumber melambat, dan panggilan tidak pernah menumpuk.  
* Setiap panggilan keluar diberi timeout eksplisit. Panggilan yang melewati timeout dibatalkan dan dihitung sebagai kegagalan.  
* Setiap poller dibungkus circuit breaker tersendiri.

Model konkurensi yang digunakan adalah asynchronous non-blocking I/O: menunggu respons jaringan tidak memblokir thread, sehingga poller dan penanganan permintaan client dapat berjalan bersamaan tanpa saling menahan.

### **7.1.3 Pembatasan Konkurensi** {#7.1.3-pembatasan-konkurensi}

Untuk mencegah lonjakan akses menghabiskan resource, sistem menerapkan batas di beberapa lapisan: 

* Global concurrency cap (Client-Facing API dan read API Aggregator): jumlah permintaan yang diproses bersamaan dibatasi oleh semaphore. Permintaan yang datang saat kapasitas penuh langsung ditolak dengan HTTP 429 beserta header Retry-After (load shedding).   
* Rate limit per client (Client-Facing API): setiap identitas client (Media, Tim Lapangan, BNPB Ops) dibatasi dengan algoritma token bucket. Lapisan ini berfungsi sebagai pembagi kapasitas yang adil agar satu client tidak memonopoli sistem, bukan sebagai pelindung kapasitas utama.   
* Timeout per hop: setiap panggilan Client-Facing API \-\> Aggregator dan Aggregator \-\> Canonical Store memiliki batas waktu, sehingga tidak ada permintaan yang menggantung dan menahan resource.   
* Connection pool terbatas: koneksi HTTP ke Aggregator dan koneksi ke Canonical Store memakai pool berukuran tetap.

### **7.1.4 Status Kebaruan Data dan Degradasi** {#7.1.4-status-kebaruan-data-dan-degradasi}

Aggregator menyimpan status untuk setiap sumber

| Attributes | Value |
| ----- | ----- |
| source | BMKG | PVMBG |
| status | UP | DEGRADED | DOWN |
| last\_success\_at | Waktu polling terakhir yang berhasil |
| stale\_since | Waktu data mulai dianggap basi (null bila UP) |
| consecutive\_fail | jumlah kegagalan polling berturut-turut |

Status ini dikendalikan oleh circuit breaker per sumber: 

* CLOSED: polling berjalan normal, status UP.   
* OPEN: setelah N kegagalan berturut-turut, breaker terbuka dan status menjadi DOWN. Polling ke sumber dihentikan sementara (fail fast) agar resource tidak terbuang untuk sumber yang mati, dan stale\_since diisi dengan last\_success\_at.   
* HALF-OPEN: setelah durasi tertentu, satu panggilan percobaan dikirim. Jika berhasil, breaker kembali CLOSED, status kembali UP, dan stale\_since dikosongkan tanpa restart. Jika gagal, breaker kembali OPEN.

Perilaku respons berdasarkan kondisi sumber:

| Kondisi | Respons |
| ----- | ----- |
| Sumber UP  | 200, data terbaru |
| Sumber DOWN, store berisi data | 200, data terakhir \+ meta.stale\_since |
| Sumber DOWN, store kosong tersedia | 503, body exkplisit: sumber tidak tersedia |
| Permintaan untuk sumber lain  | tidak terpengaruh |

Setiap respons menyertakan metadata kebaruan, contoh:

| { "data": \[ ...HazardEvent... \], "meta": {      "source": "PVMBG",      "source\_status": "DOWN",      "last\_success\_at": "2026-10-01T10:15:02Z",      "stale\_since": "2026-10-01T10:15:02Z" } } |
| :---- |

Dengan demikian, selama outage PVMBG permintaan seismik tetap dilayani normal, permintaan vulkanik dijawab dengan data terakhir beserta penanda kebaruan, tidak ada respons 500, dan tidak ada permintaan yang menggantung hingga timeout client.

### **7.1.5 Parameter Konfigurasi** {#7.1.5-parameter-konfigurasi}

Seluruh parameter dibaca dari environment variable:

| Variabel | Default | Keterangan |
| ----- | ----- | ----- |
| POLL\_INTERVAL\_MS | 3000 | Interval polling per sumber |
| POLL\_TIMEOUT\_MS | 4000 | Timeout satu panggilan polling |
| CB\_FAILURE\_THRESHOLD | 3 | Kegagalan berturut-turut sebelum OPEN |
| CB\_OPEN\_DURATION\_MS | 10000 | Durasi OPEN sebelum HALF-OPEN |
| MAX\_INFLIGHT\_REQUESTS | 100 | Global concurrency cap (Semaphore) |
| RATE\_LIMIT\_RPS | 50 | Token bucket per client |
| UPSTREAM\_TIMEOUT\_MS | 1000 | Timeout Client APi \-\> Aggregator |
| DB\_POOL\_SIZE | 20 | Ukuran connection pool Canonical Store |

## **7.2 Alasan Keputusan** {#7.2-alasan-keputusan}

### **7.2.1 Pemisahan Jalur Ingest dan Jalur Akses** {#7.2.1-pemisahan-jalur-ingest-dan-jalur-akses}

Alternatif yang dipertimbangkan: 

* Proxy langsung: setiap permintaan client diteruskan secara real-time ke BMKG/PVMBG. Ini adalah pendekatan prototipe awal dan merupakan sumber utama head-of-line blocking, karena latensi client ikut menanggung latensi sumber yang paling lambat.  
* Scatter-gather parallel per permintaan: BMKG dan PVMBG dipanggil paralel saat ada permintaan. Pendekatan ini menghilangkan urutan sekuensial, tetapi permintaan yang membutuhkan PVMBG tetap menunggu hingga 3 detik, setiap permintaan client melipatgandakan beban ke sumber, dan outage sumber langsung terasa oleh client.  
* Cache dengan TTL di depan sumber: mengurangi beban, tetapi saat cache kedaluwarsa permintaan kembali menunggu sumber (cache miss), sehingga latensi menjadi tidak konsisten. 

Pendekatan yang dipilih (polling di *background*, client membaca dari *Canonical Store*) dipilih karena latensi client sepenuhnya lepas dari latensi dan ketersediaan sumber. Selain itu, spesifikasi sudah menetapkan pola ingest berupa polling, sehingga *Canonical Store* secara alami menjadi sumber jawaban bagi client. 

Trade-off:   
data yang diterima client bersifat eventually consistent, dengan keterlambatan maksimal sebesar interval polling ditambah latensi sumber. Untuk kebutuhan koordinasi kebencanaan dengan interval 2-5 detik, keterlambatan ini dapat diterima. 

### **7.2.2 Model Konkurensi Asynchronous Non-Blocking I/O** {#7.2.2-model-konkurensi-asynchronous-non-blocking-i/o}

Alternatif yang dipertimbangkan: 

* Thread-per-request: sederhana, tetapi setiap permintaan yang menunggu I/O menahan satu thread beserta memorinya. Pada lonjakan akses, jumlah thread tumbuh tanpa batas dan menjadi penyebab *resource exhaustion* itu sendiri.  
* *Worker pool* dengan thread tetap: jumlah thread terkendali, tetapi thread tetap menganggur selama menunggu I/O, sehingga kapasitas efektif terbatas pada jumlah thread.

Beban kerja sistem didominasi I/O (panggilan jaringan ke sumber, Aggregator, dan Canonical Store), bukan komputasi. Asynchronous non-blocking I/O memungkinkan ribuan operasi menunggu secara bersamaan dengan biaya yang kecil per operasi, sehingga poller dan permintaan client dapat berjalan berdampingan tanpa saling menahan. 

Trade-off: seluruh operasi di dalam event loop wajib non-blocking. Satu operasi blocking atau komputasi berat dapat menahan seluruh event loop. Karena pemetaan skema bersifat ringan, risiko ini dinilai kecil.

### **7.2.3 Poller Independen dengan Single In-Flight** {#7.2.3-poller-independen-dengan-single-in-flight}

Alternatif yang dipertimbangkan: 

* Satu loop polling sekuensial (BMKG lalu PVMBG): siklus BMKG tertahan oleh PVMBG, sehingga data seismik ikut terlambat masuk ke store.   
* Satu loop yang memanggil kedua sumber secara paralel lalu menunggu keduanya: urutan tidak lagi sekuensial, tetapi siklus berikutnya tetap menunggu sumber yang paling lambat. 

Poller terpisah per sumber membuat laju ingest BMKG sama sekali tidak bergantung pada PVMBG. Aturan single in-flight dipilih agar panggilan ke sumber yang melambat tidak menumpuk dan tidak memperparah beban pada sumber tersebut. 

Trade-off: saat sumber melambat, sebagian siklus polling dilewati sehingga data baru dari sumber tersebut masuk sedikit lebih lambat.

### **7.2.4 Load Shedding pada Global Concurrency Cap** {#7.2.4-load-shedding-pada-global-concurrency-cap}

Alternatif yang dipertimbangkan: 

* Tanpa batas: setiap permintaan diterima dan diproses. Ini adalah kondisi yang menyebabkan resource exhaustion.   
* Antrian tanpa batas: permintaan tidak ditolak, tetapi antrian tumbuh terus saat lonjakan, latensi meningkat tanpa batas, dan memori habis.   
* Antrian berbatas dengan waktu tunggu: permintaan menunggu giliran, tetapi menambah latensi pada semua permintaan saat sistem sibuk. 

Penolakan langsung dengan HTTP 429 dipilih karena menjaga latensi permintaan yang diterima tetap rendah dan memberi sinyal yang jelas kepada client untuk mencoba kembali (Retry-After). Sistem tetap melayani sebagian permintaan dengan baik. 

Trade-off: sebagian client menerima 429 saat puncak beban dan perlu melakukan retry.

### **7.2.5 Rate Limiting Token Bucket per Client** {#7.2.5-rate-limiting-token-bucket-per-client}

Alternatif yang dipertimbangkan: 

* Fixed window: sederhana, tetapi memungkinkan lonjakan dua kali lipat di sekitar batas window.   
* Sliding window log: akurat, tetapi harus menyimpan timestamp setiap permintaan sehingga boros memori.  
* Leaky bucket: menghasilkan laju keluaran yang rata, tetapi permintaan diantrikan terlebih dahulu sehingga menambah latensi dan berisiko melanggar target p95 \< 300 ms.   
* Sliding window counter: menyeimbangkan akurasi dan memory dengan weighted average antar window, tetapi hasilnya tetap estimasi (asumsi distribusi request merata di window sebelumnya) sehingga bisa sedikit meleset saat traffic bursty/tidak uniform.

Token bucket dipilih karena mengizinkan burst singkat yang wajar, tidak menambah latensi pada permintaan yang lolos, dan hanya membutuhkan dua nilai per client (jumlah token dan waktu isi ulang terakhir). 

Kunci rate limit berupa client\_id dari klaim JWT, bukan alamat IP, karena beberapa client dapat berbagi IP yang sama (misalnya di balik NAT atau saat load testing) dan identitas client sudah terverifikasi oleh Client-Facing API.

Trade-off: state token bucket disimpan in-memory sehingga hilang saat Client-Facing API di-restart dan tidak dapat dibagi antar-instance. Karena Client-Facing API berjalan sebagai satu instance pada M1, hal ini dapat diterima (lihat [7.3](#7.3-batasan-yang-disadari)).

### **7.2.6 Circuit Breaker dan Respons Stale dengan Penanda** {#7.2.6-circuit-breaker-dan-respons-stale-dengan-penanda}

Alternatif yang dipertimbangkan: 

* Mengembalikan error saat sumber mati: sederhana, tetapi membuang data terakhir yang masih berguna. Ini adalah perilaku yang menyebabkan dashboard BNPB kosong.  
* Menyajikan data terakhir tanpa penanda: client tidak dapat membedakan data terkini dari data basi, yang berbahaya dalam konteks kebencanaan karena status gunung api dapat berubah selama outage.  
* Retry terus-menerus tanpa circuit breaker: membuang resource untuk sumber yang sudah jelas mati dan memperbanyak error log.

Kombinasi circuit breaker dan respons stale dengan penanda (stale\_since, source\_status) dipilih karena data terakhir tetap tersedia bagi client, client mengetahui secara eksplisit sejak kapan data tersebut tidak diperbarui, dan sistem berhenti membuang resource untuk sumber yang mati. Status HALF-OPEN memungkinkan pemulihan otomatis tanpa restart ketika sumber kembali hidup.   
Respons 503 dengan body eksplisit dipilih untuk kondisi store kosong, karena mengembalikan 200 dengan data kosong dapat disalahartikan sebagai "tidak ada aktivitas vulkanik". 

Trade-off: deteksi outage tidak instan, melainkan membutuhkan N kegagalan berturut-turut (CB\_FAILURE\_THRESHOLD). Pemulihan juga tertunda hingga durasi OPEN berakhir (CB\_OPEN\_DURATION\_MS). Nilai keduanya dapat dikonfigurasi untuk menyeimbangkan kecepatan deteksi dan ketahanan terhadap gangguan sesaat.

## **7.3 Batasan Yang Disadari** {#7.3-batasan-yang-disadari}

### **7.3.1 Data Bersifat Eventual Consistent** {#7.3.1-data-bersifat-eventual-consistent}

Client membaca dari Canonical Store, sehingga data yang diterima tertinggal paling lama sebesar interval polling ditambah latensi sumber. Hal ini merupakan konsekuensi dari pola ingest polling yang ditetapkan spesifikasi, dan dengan interval 2-5 detik keterlambatan ini masih dapat diterima untuk kebutuhan koordinasi.

### **7.3.2 Deteksi Outage Tidak Instan** {#7.3.2-deteksi-outage-tidak-instan}

Circuit breaker baru terbuka setelah CB\_FAILURE\_THRESHOLD kegagalan berturut-turut, sehingga selama jeda tersebut data vulkanik masih disajikan dengan status UP. Ambang ini sengaja dipasang agar gangguan sesaat tidak langsung dianggap outage. Setelah status berubah, stale\_since tetap akurat karena diisi dengan last\_success\_at.

### **7.3.3 Sumber yang Sangat Lambat Diperlakukan Sama dengan Sumber yang Mati** {#7.3.3-sumber-yang-sangat-lambat-diperlakukan-sama-dengan-sumber-yang-mati}

Panggilan yang melewati POLL\_TIMEOUT\_MS dihitung sebagai kegagalan, karena sumber yang lambat maupun mati sama-sama tidak menghasilkan data tepat waktu. Nilai default timeout sengaja diatur di atas delay maksimum PVMBG pada spesifikasi (3 detik). 

### **7.3.4 Load shedding tidak memprioritaskan client** {#7.3.4-load-shedding-tidak-memprioritaskan-client}

Global concurrency cap berlaku sama untuk semua client, sehingga saat beban puncak permintaan Tim Lapangan dapat ditolak sama seperti permintaan Media. 

### **7.3.5 Satu instance per service dan state in-memory** {#7.3.5-satu-instance-per-service-dan-state-in-memory}

Client-Facing API dan Aggregator masing-masing berjalan sebagai satu instance, dengan state token bucket dan status sumber disimpan in-memory. State ini hilang saat restart dan akan terbentuk ulang dari permintaan dan hasil polling berikutnya. Skala horizontal membutuhkan penyimpanan state bersama (misalnya Redis) yang menambah komponen baru, sedangkan satu instance sudah cukup untuk kriteria M1.

### **7.3.6 Aggregator dan Canonical Store Merupakan Dependensi Jalur Akses** {#7.3.6-aggregator-dan-canonical-store-merupakan-dependensi-jalur-akses}

Jika Aggregator atau Canonical Store mati, seluruh permintaan data tidak dapat dilayani. Client-Facing API mengembalikan 503 eksplisit tanpa menggantung, tetapi tidak menyediakan data cadangan.

### **7.3.7 Hasil load testing bergantung pada lingkungan pengujian** {#7.3.7-hasil-load-testing-bergantung-pada-lingkungan-pengujian}

Load testing dijalankan pada satu mesin yang juga menjalankan seluruh container, sehingga load generator dan sistem berbagi CPU dan memori. Angka yang dilaporkan bertujuan membuktikan perilaku sistem di bawah beban, bukan mengukur kapasitas produksi.

## **7.4 Bukti Penyelesaian** {#7.4-bukti-penyelesaian}

### **7.4.1 Uji p95 BMKG-only saat PVMBG delay 3 detik** {#7.4.1-uji-p95-bmkg-only-saat-pvmbg-delay-3-detik}

\<placeholder\> 

### **7.4.2 Load Testing (≥50 koneksi, 60 detik)** {#7.4.2-load-testing-(≥50-koneksi,-60-detik)}

\<placeholder\> 

### **7.4.3 Demo Outage PVMBG** {#7.4.3-demo-outage-pvmbg}

\<placeholder\> 

## **7.5 Model Konkurensi dan Mitigasi Kegagalan** {#7.5-model-konkurensi-dan-mitigasi-kegagalan}

### **7.5.1 Model Konkurensi** {#7.5.1-model-konkurensi}

Sistem menggunakan asynchronous concurrency berbasis non-blocking I/O. Beban kerja sistem didominasi operasi I/O (panggilan jaringan ke sumber, Aggregator, dan Canonical Store), bukan komputasi, sehingga parallelism tidak menjadi fokus rancangan. Poller BMKG, poller PVMBG, dan penanganan permintaan client berjalan sebagai task asinkron independen di atas event loop, dengan jumlah permintaan bersamaan dibatasi oleh semaphore.

### **7.5.2 Mitigasi Head-of-Line Blocking** {#7.5.2-mitigasi-head-of-line-blocking}

Client tidak pernah memicu panggilan langsung ke BMKG maupun PVMBG. Permintaan client dijawab Aggregator dari Canonical Store, sehingga latensi PVMBG tidak masuk ke jalur permintaan client. Poller per sumber juga berjalan independen, sehingga ingest data seismik tidak tertahan oleh PVMBG yang lambat ([7.1.1](#7.1.1-pemisahan-jalur-ingest-dan-jalur-pelayanan), [7.1.2](#7.1.2-poller-independen-per-sumber)).

### **7.5.3 Mitigasi Resource Exhaustion** {#7.5.3-mitigasi-resource-exhaustion}

Semaphore membatasi jumlah permintaan yang diproses bersamaan, dan permintaan yang melebihi kapasitas langsung ditolak dengan HTTP 429\. Rate limiting per client dengan token bucket mencegah satu client memonopoli kapasitas, sementara timeout di setiap hop dan connection pool terbatas mencegah permintaan menggantung dan menahan resource ([7.1.3](#7.1.3-pembatasan-konkurensi)). 

### **7.5.4 Mitigasi Ketiadaan Degradasi Terkendali** {#7.5.4-mitigasi-ketiadaan-degradasi-terkendali}

Circuit breaker per sumber mendeteksi sumber yang mati secara otomatis di level polling, lalu mencatat stale\_since. Selama sumber DOWN, data sumber tersebut dijawab dari Canonical Store beserta stale\_since, sedangkan sumber lain tidak terpengaruh. Setelah sumber hidup kembali, circuit breaker memulihkan status secara otomatis tanpa restart ([7.1.4](#7.1.4-status-kebaruan-data-dan-degradasi)).

## **7.6 Konfigurasi dan Tooling Load Testing** {#7.6-konfigurasi-dan-tooling-load-testing}

# **BAB VIII** **Problem 3 \- Service-to-Service Authentication & Trust** {#bab-viii-problem-3---service-to-service-authentication-&-trust}

## **8.1 Rancangan Solusi** {#8.1-rancangan-solusi}

### **8.1.1 Otentikasi dan Otorisasi antar Servis** {#8.1.1-otentikasi-dan-otorisasi-antar-servis}

**Flow PVMBG:**

1. PVMBG menyediakan layanan untuk mengakses *resource protected* mereka. Perlu daftar akun terlebih dahulu.  
2. Setelah daftar akun dan mengisi beberapa data berkaitan dengan instansi/perusahaan untuk memverifikasi penggunaan, admin PVMBG akan memverifikasi pendaftaran.  
3. Apabila sudah terverifikasi, BNPB akan mendapat client ID dan client secret.  
4. Setelah client ID dan client secret didapatkan, BNPB *generate* one time code (misalnya melalui web interface PVMBG) agar bisa mendapat access token untuk pertama kalinya:

| `URL` | `/oauth/token` |
| :---- | :---- |
| `Method` | `HTTP POST` |
| **`Header`** |  |
| `Authorization` | `Basic Y2xpZW50X2lkOmNsaWVudF9zZWNyZXQ=` |
| **`Request Body`** |  |
| `code` | `<one_time_code>` |
| `grant_type` | `authorization_code` |

   Dengan authorization header bernilai \<client\_id\>:\<client\_secret\> dalam format base64

   Akan dikembalikan:

| `{     "access_token" : "<access_token>",     "refresh_token" : "<refresh_token>",     "expires_in" : 60 }` |
| :---- |

   Dengan ketentuan bahwa access token yang dipakai oleh service BNPB untuk mengakses resource (dalam Authorization: Bearer). Selanjutnya, refresh token dipakai untuk mendapatkan access token baru (asumsinya satu client hanya mempunyai satu access token yang valid dalam waktu yang sama):

| `URL` | `/oauth/token` |
| :---- | :---- |
| `Method` | `HTTP POST` |
| **`Header`** |  |
| `Authorization` | `Basic Y2xpZW50X2lkOmNsaWVudF9zZWNyZXQ=` |
| **`Request Body`** |  |
| `grant_type` | `refresh_token` |
| `refresh_token` | `<refresh_token>` |

	Akan dikembalikan:

| `{     "access_token" : "<new_access_token>",     "refresh_token" : "<new_refresh_token>",     "expires_in" : 60 }` |
| :---- |

5. Untuk memverifikasi apakah token itu benar-benar berasal dari server third party atau tidak, client dapat menggunakan api berikut ini:

| `URL` | `/oauth/introspect` |
| :---- | :---- |
| `Method` | `HTTP POST` |
| **`Header`** |  |
| `Authorization` | `Basic Y2xpZW50X2lkOmNsaWVudF9zZWNyZXQ=` |
| **`Request Body`** |  |
| `access_token` | `<access_token>` |

	Akan dikembalikan:

| `{     "active" : <boolean>,     "expires_in" : 60 }` |
| :---- |

	Catatan: Expired in berarti token akan kadarluasa dalam 60 detik (untuk keperluan demonstrasi).  
Agar token dan refresh token persists through multiple deployment, volume perlu dikonfigurasi dalam file docker compose.

**Flow BMKG:**

1. BMKG menyediakan servis developer API. BNPB perlu daftar akun developer terlebih dahulu dan verifikasi perusahaan/instansi.  
2. Apabila sudah terverifikasi, BNPB dapat create new API key yang diberi nama (mis. bnpb-app-key). API key hanya muncul sekali di interface, sehingga BNPB perlu copy key tersebut dan simpan dalam .env mereka.  
3. API key dapat dipakai dengan menyertakan header X-BMKG-Key dalam GET HTTP request ke BMKG. Tanpa X-BMKG-Key, permintaan akan ditolak.  
4. API Key dapat direvoke kapanpun oleh akun BNPB.

### **8.1.2 Otentikasi dan Otorisasi Client ke Service** {#8.1.2-otentikasi-dan-otorisasi-client-ke-service}

BNPB mempunyai IdP sendiri untuk otorisasi dan otentikasi client downstream yang menggunakan HMAC Signed JWT sebagai token otorisasinya. Pengguna dapat mendaftarkan akun sebagai media. Akun operator di-*seed* ke dalam database, sedangkan akun lapangan ditambahkan oleh operator. Diasumsikan tidak perlu verifikasi email untuk pendaftaran akun. Setelah akun berhasil didaftar, pengguna dapat login menggunakan email dan password masing-masing. Flow requestnya:

1. Pengguna request login  
2. Request masuk ke API Gateway  
3. Request login diarahkan ke IdP  
4. IdP verifikasi email dan hash(password \+ secret\_salt)  
5. Jika password benar, IdP mengembalikan set-cookie HttpOnly sebuah JWT berupa

| `Header: { "alg": "HS256", "typ": "JWT" } Payload: {   "name": "Jane Doe",   "email": "jane@example.com",   "role": "Media/Lapangan/Operator",   "iat": 1727360297,   "exp": 1727360357 } Base64HeaderAndPayload = Base64(Header) + “.” + Base64(Payload) Signature: HMACSHA256(Base64HeaderAndPayload) Yang dikembalikan: Base64HeaderAndPayload + “.” + Signature` |
| :---- |

   Nilai exp (expired at, unix timestamp) akan bergantung dengan rolenya. Untuk tim lapangan akan bernilai iat \+ 60 detik.

6. Setelah login, setiap client request menggunakan cookie tersebut. Setiap request masuk API Gateway, JWT langsung diverifikasi oleh API Gateway.  
7. Jika JWT valid, request langsung diteruskan ke service BNPB

Selain itu terdapat mekanisme refresh token. JWT dapat direfresh menggunakan refresh token. Refresh token bersifat stateful yang disimpan dalam user DB dalam IdP. Jadi skema database akan seperti:

| `CREATE TABLE refresh_tokens (        refresh_token        VARCHAR(64) PRIMARY KEY,        user_id              VARCHAR(255) NOT NULL,                FOREIGN KEY (user_id) REFERENCES user(user_id) )` |
| :---- |

Panjang Varchar(64) merupakan contoh saja. Saat login, refresh token juga di set-cookie bersama JWT. Refresh token yang dikembalikan juga disimpan dalam DB seperti skema di atas. Selama pengguna tidak logout, refresh token yang disimpan dalam DB akan terus ada. Refresh token dalam DB hanya dihapus dalam:

1. Request refresh JWT (technically diupdate sih), dan  
2. Logout akun

Request refresh JWT itu harus otomatis terjadi setiap kali pengguna mengirim sebuah request tetapi JWT itu expired. Flownya adalah seperti berikut ini:

1. Pengguna mengirim request dengan JWT yang expired bersama refresh token yang valid.  
2. Client Facing API mendeteksi bahwa JWT tidak valid. Request disimpan dalam memory, kemudian memanggil endpoint /refresh IdP dengan hanya meneruskan refresh token data nya.  
3. IdP fetch where refresh\_token \= refresh\_token dalam DB sambil join tabel user. Apabila ada, artinya refresh token masih valid. Kemudian refresh token bersama JWT-nya dirotasi. Refresh token lama diupdate menjadi refresh token baru, kemudian JWT dan refresh token baru dikembalikan ke Client Facing API.  
4. Gateway menerima JWT dan refresh token baru, kemudian inject JWT yang valid ke GET Request. Request tersebut diteruskan ke servis berikutnya.  
5. Setelah menerima response, Client Facing API mengembalikan response ke client sambil set-cookie JWT  dan refresh token yang baru  
6. Client (browser) memperbaharui JWT dan refresh token secara otomatis

## **8.2 Alasan Keputusan** {#8.2-alasan-keputusan}

Asumsi utama dalam perancangan autentikasi antar-service adalah bahwa BMKG dan PVMBG merupakan layanan pihak ketiga yang otonom. BNPB tidak mengendalikan cara kedua instansi menerbitkan maupun memvalidasi kredensial, sehingga BNPB diposisikan sebagai client yang mengikuti mekanisme masing-masing instansi. Spesifikasi juga mewajibkan kredensial kedua instansi memiliki format berbeda, sehingga setiap instansi membentuk domain of trust tersendiri.

### **8.2.1 Mekanisme Antar-Service: OAuth 2.0 vs SAML vs OpenID Connect vs Shared Key** {#8.2.1-mekanisme-antar-service:-oauth-2.0-vs-saml-vs-openid-connect-vs-shared-key}

* SAML dirancang untuk Single Sign-On pengguna berbasis browser dengan pertukaran dokumen XML, sehingga tidak ditujukan untuk komunikasi antar mesin.  
* OpenID Connect menambahkan lapisan identitas pengguna di atas OAuth 2.0. BNPB tidak perlu membuktikan identitas pengguna ke PVMBG, melainkan hanya membutuhkan otorisasi akses resource.  
* Shared key (API key statis) paling sederhana dan dipakai BMKG sesuai kontrak spesifikasi, tetapi tidak memiliki masa berlaku, sehingga key yang bocor tetap valid hingga dicabut manual.  
* OAuth 2.0 menghasilkan access token berumur pendek yang terpisah dari kredensial utama. 

OAuth 2.0 dipilih untuk PVMBG karena memenuhi kewajiban Bearer token dengan struktur tersendiri sekaligus membatasi dampak token yang bocor. Grant authorization\_code dengan rotasi refresh token dipilih dibandingkan client\_credentials karena untuk memperoleh token baru dibutuhkan client secret sekaligus refresh token yang valid. Rotasi refresh token juga membuat penggunaan token curian tidak valid lagi.

Trade-off: Aggregator harus mengelola siklus hidup token, yaitu memperbarui access token sebelum kedaluwarsa dan menyimpan refresh token hasil rotasi ke volume sebelum digunakan. Client secret tetap merupakan kredensial jangka panjang, dampak kebocorannya dibatasi dengan kemampuan PVMBG untuk mencabutnya dan kebutuhan one time code untuk mengakses resource.

### **8.2.2 Format Access Token PVMBG: Opaque Token vs JWT** {#8.2.2-format-access-token-pvmbg:-opaque-token-vs-jwt}

Opaque token membutuhkan pemeriksaan ke penyimpanan token pada setiap permintaan, tetapi dapat dicabut seketika. JWT memuat klaim yang ditandatangani, sehingga PVMBG dapat memvalidasi token hanya dengan memeriksa signature dan klaim exp tanpa mengakses penyimpanan token. 

JWT dipilih karena validasi tidak membutuhkan lookup ke penyimpanan pada setiap permintaan polling, dan struktur token (header, payload, signature) memenuhi ketentuan Bearer token dengan struktur tersendiri yang berbeda dari API key BMKG. 

Trade-off: JWT tidak dapat dicabut sebelum kedaluwarsa. Dampaknya dibatasi oleh masa berlaku access token yang pendek (60 detik untuk akun lapangan). Isi payload juga dapat dibaca oleh pemegang token, sehingga payload dibatasi agar tidak memuat data rahasia.

### **8.2.3 Autentikasi Client ke BNPB: JWT vs Session-Based** {#8.2.3-autentikasi-client-ke-bnpb:-jwt-vs-session-based}

Session-based memudahkan revokasi, tetapi mewajibkan Client-Facing API memeriksa sesi ke Auth/Token Service pada setiap permintaan. Hal ini menambah satu hop jaringan dan menjadikan Auth/Token Service titik kegagalan bagi seluruh permintaan. 

JWT dipilih untuk access token karena dapat diverifikasi secara lokal di masing-masing servis sehingga tidak menambah latensi dan permintaan dengan token yang masih valid tetap dilayani walaupun Auth/Token Service sedang di-restart. Keterbatasan JWT dalam hal revokasi diimbangi dengan masa berlaku yang dapat dikonfigurasi per role dan refresh token yang bersifat stateful di User DB. Dengan demikian, validasi access token bersifat stateless, sedangkan pengelolaan sesi jangka panjang bersifat stateful. 

Payload JWT hanya dapat dibaca, bukan dienkripsi. Karena itu, payload tidak memuat data rahasia, dan integritasnya dan authenticity-nya dijaga oleh HMAC signature.

### **8.2.4 Mekanisme Tanda Tangan JWT: Hashed-based Message Authentication Code vs Digital Signature** {#8.2.4-mekanisme-tanda-tangan-jwt:-hashed-based-message-authentication-code-vs-digital-signature}

* Hashed-based Message Authentication Code (HMAC, misalnya HS256): simetris. Penerbit dan verifier memegang secret yang sama. Cepat, token berukuran kecil, dan sederhana dikonfigurasi. Konsekuensinya, pihak yang dapat memverifikasi token juga dapat menerbitkannya.  
* Digital signature (RSA seperti RS256, atau ECDSA seperti ES256): asimetris. Token ditandatangani dengan private key yang hanya dipegang penerbit, dan diverifikasi dengan public key. Verifier tidak dapat menerbitkan token. Di antara keduanya, ECDSA menghasilkan key dan signature yang lebih kecil, sedangkan RSA lebih luas dukungannya.

HMAC dipilih karena seluruh service termasuk client facing API, agregator, canonical store, dan user service merupakan sistem internal BNPB. Tidak ada keperluan untuk separate private dan public key karena sistem tidak akan share key manapun ke luar sistem.

Trade-off: kebocoran secret dari Client-Facing API memungkinkan penerbitan token dengan role apa pun. Risiko ini dinilai dapat diterima karena Client-Facing API adalah satu-satunya verifier, dan kompromi pada komponen tersebut sudah membuka akses ke data yang melewatinya. Digital signature menjadi pilihan apabila jumlah verifier bertambah atau verifier berada di luar domain of trust Auth/Token Service.

### **8.2.5 Mekanisme Refresh: Client-Side Refresh vs Transparent Refresh** {#8.2.5-mekanisme-refresh:-client-side-refresh-vs-transparent-refresh}

Pada client-side refresh, client mendeteksi respons 401, memanggil endpoint refresh secara eksplisit, lalu mengulang permintaan. Alur ini mudah diamati, tetapi setiap client wajib mengimplementasikan logika refresh dan token harus dapat diakses oleh kode client.

Transparent refresh dipilih: Client-Facing API mendeteksi JWT yang kedaluwarsa, menukar refresh token ke Auth/Token Service, meneruskan permintaan dengan JWT baru, lalu mengembalikan token baru melalui cookie. Client tidak perlu mengimplementasikan logika refresh, dan kedua token disimpan sebagai cookie HttpOnly sehingga tidak dapat dibaca JavaScript dan terlindung dari pencurian melalui XSS. Alur ini memenuhi ketentuan refresh tanpa login ulang manual.

Trade-off: permintaan dengan JWT kedaluwarsa bergantung pada ketersediaan Auth/Token Service, dan permintaan paralel yang membawa refresh token yang sama dapat saling bertabrakan karena hanya satu yang berhasil melakukan rotasi (lihat 8.3).

## **8.3 Batasan Yang Disadari** {#8.3-batasan-yang-disadari}

Masalah concurrency JWT refresh.

## **8.4 Bukti Penyelesaian** {#8.4-bukti-penyelesaian}

### **8.4.1 Kredensial BMKG Ditolak di PVMBG (dan sebaliknya)** {#8.4.1-kredensial-bmkg-ditolak-di-pvmbg-(dan-sebaliknya)}

\<placeholder\> 

### **8.4.2 Scope Media Ditolak Akses Field Mentah** {#8.4.2-scope-media-ditolak-akses-field-mentah}

\<placeholder\>

### **8.4.3 Token Expiry \+ Refresh Flow Tim Lapangan** {#8.4.3-token-expiry-+-refresh-flow-tim-lapangan}

\<placeholder\> 

### **8.4.4 Tidak Ada Hardcoded Credential** {#8.4.4-tidak-ada-hardcoded-credential}

\<placeholder\> 

## **8.5 Mekanisme yang dipakai beserta alasan mengapa mekanisme tersebut tidak membuka celah baru.** {#8.5-mekanisme-yang-dipakai-beserta-alasan-mengapa-mekanisme-tersebut-tidak-membuka-celah-baru.}

Dijelaskan di alasan keputusan dan rancangan solusi.

# **BAB IX**  **Problem 4 \- Arsitektur Microservice & Flexible Storage** {#bab-ix-problem-4---arsitektur-microservice-&-flexible-storage}

## **9.1 Rancangan Solusi** {#9.1-rancangan-solusi}

Terdapat tiga layanan utama yang relevan dengan Problem 4, yaitu Aggregator, Client-Facing API, dan Auth/Token Service, serta sekelompok event consumer yang terhubung melalui message broker.

Pertama, Aggregator merupakan satu-satunya komponen yang berkomunikasi langsung dengan BMKG dan PVMBG. Komponen ini melakukan polling ke kedua sumber, menerapkan pemetaan yang toleran terhadap perubahan skema sebagaimana didefinisikan pada [Problem 1](#bab-vi-problem-1---arsitektur-&-interoperabilitas-data) (field yang tidak dikenal atau baru ditambahkan, seperti confidence\_level, diarahkan ke field attributes), dan merupakan pemilik tunggal Canonical Store. Tidak ada service lain yang memiliki kredensial database maupun connection string ke penyimpanan ini. Selain itu, Canonical Store ditempatkan pada jaringan Docker internal yang hanya diikuti oleh Aggregator, sehingga service lain tidak dapat menjangkaunya secara jaringan.

Kedua, Client-Facing API merupakan proxy read-only. Komponen ini tidak pernah meng-query Canonical Store secara langsung, setiap permintaan data dilayani dengan memanggil API milik Aggregator. Permintaan client diautentikasi dengan memverifikasi JWT secara lokal menggunakan signature dan klaim exp milik token tersebut. Untuk permintaan dengan token yang masih valid, tidak ada panggilan ke Auth/Token Service pada jalur permintaan. Client-Facing API hanya menghubungi Auth/Token Service pada dua kondisi: meneruskan permintaan login, dan melakukan refresh secara transparan ketika access token client telah kedaluwarsa (lihat [8.1.2](#8.1.2-otentikasi-dan-otorisasi-client-ke-service)).

Ketiga, Auth/Token Service bertanggung jawab menerbitkan JWT bertanda tangan dengan TTL pendek yang dapat dikonfigurasi (default 60 detik), serta mengelola refresh token. Komponen ini memiliki penyimpanannya sendiri, yaitu User DB, yang berisi data akun dan refresh token dan tidak diakses oleh service lain. Auth/Token Service tidak memiliki ketergantungan terhadap Aggregator, karena Aggregator tidak pernah menerbitkan maupun mengonsumsi token client. Hubungannya dengan Client-Facing API bersifat satu arah dan terbatas pada login dan refresh.

Terakhir, event consumer (Dashboard, Notifier, dan Audit Logger) berjalan sebagai service terpisah yang berlangganan HazardEvent melalui RabbitMQ. Aggregator hanya mengenal exchange, bukan consumer, sehingga consumer dapat dihentikan, di-rebuild, atau ditambahkan tanpa memengaruhi Aggregator (lihat [Problem 5](#bab-x-problem-5---fan-out-event-distribution/pub-sub)).

HazardEvent disimpan di PostgreSQL dengan skema *hybrid*. Sepuluh field kanonik yang strukturnya ditetapkan spesifikasi (hazard\_id, source, source\_ref\_id, hazard\_type, severity, area\_name, latitude, longitude, occurred\_at, ingested\_at) disimpan sebagai kolom bertipe. Constraint NOT NULL hanya diterapkan pada field wajib (hazard\_id, source, source\_ref\_id, hazard\_type, occurred\_at, ingested\_at), sedangkan field lain bersifat nullable sesuai kebijakan pada [Problem 1](#bab-vi-problem-1---arsitektur-&-interoperabilitas-data). Kombinasi (source, source\_ref\_id) diberi unique constraint untuk mencegah duplikasi record akibat polling berulang. Field attributes, yang diperuntukkan bagi atribut spesifik sumber tanpa padanan kanonik, disimpan sebagai kolom JSONB, sehingga field baru seperti confidence\_level dapat tersimpan tanpa migrasi skema.

## **9.2 Alasan Keputusan** {#9.2-alasan-keputusan}

Keputusan pada [Problem 4](#bab-ix-problem-4---arsitektur-microservice-&-flexible-storage) mencakup dua hal: penentuan batas service dan cara HazardEvent disimpan. Perbandingan alternatif secara lengkap dijabarkan pada [9.5](#9.5-batas-service-dan-alternatif-penyimpanan).

### **9.2.1 Batas Service** {#9.2.1-batas-service}

Sistem dipecah berdasarkan tanggung jawab fungsional, bukan dipertahankan sebagai monolith. Pada monolith, polling, pemetaan, autentikasi, dan penyajian berada dalam satu unit deploy, sehingga perubahan kecil pada autentikasi memaksa seluruh sistem di-deploy ulang, termasuk bagian pemetaan yang tidak berubah. Pemecahan dilakukan mengikuti tiga pertimbangan: 

* Laju perubahan yang berbeda: logika pemetaan berubah mengikuti skema sumber, sedangkan logika autentikasi berubah mengikuti kebijakan keamanan. Keduanya perlu dapat di-deploy terpisah.  
* Domain of trust yang berbeda: hanya Aggregator yang memegang kredensial BMKG dan PVMBG, dan hanya Auth/Token Service yang memegang kunci penandatanganan token. Pemisahan service membatasi dampak apabila salah satu komponen terkompromi.  
* Isolasi kegagalan: matinya satu service tidak menghentikan service lain yang tidak bergantung padanya. 

Salah satu konsekuensi penting dari batas ini adalah verifikasi JWT dilakukan secara lokal di Client-Facing API. Alternatifnya, memanggil Auth/Token Service secara sinkron pada setiap permintaan, ditolak karena menciptakan coupling langsung, restart Auth/Token Service akan melumpuhkan seluruh lalu lintas Client-Facing API. Dengan verifikasi lokal, ketergantungan Client-Facing API terhadap Auth/Token Service terbatas pada login dan refresh. Selama Auth/Token Service di-restart, permintaan dengan token yang masih valid tetap dilayani, hanya login dan refresh yang gagal sementara dan dijawab dengan 503 eksplisit.

## **9.3 Batasan Yang Disadari** {#9.3-batasan-yang-disadari}

### **9.3.1 Restart Auth/Token Service berdampak pada login dan refresh** {#9.3.1-restart-auth/token-service-berdampak-pada-login-dan-refresh}

Permintaan dengan token yang masih valid tetap dilayani, tetapi login dan refresh gagal sementara dengan 503 selama Auth/Token Service di-restart.

### **9.3.2 Aggregator merupakan dependensi utama jalur data** {#9.3.2-aggregator-merupakan-dependensi-utama-jalur-data}

Restart Aggregator menghentikan sementara ingest dan permintaan data, yang dijawab Client-Facing API dengan 503 eksplisit. Login, refresh, dan consumer tetap berjalan. 

### **9.3.3 Canonical Store berjalan sebagai satu instance** {#9.3.3-canonical-store-berjalan-sebagai-satu-instance}

PostgreSQL berjalan tanpa replikasi, sehingga kegagalan database menghentikan jalur data. Data tetap aman karena tersimpan pada Docker volume.

### **9.3.4 Fleksibilitas skema hanya berlaku untuk atribut sumber** {#9.3.4-fleksibilitas-skema-hanya-berlaku-untuk-atribut-sumber}

Field baru dari sumber diserap attributes tanpa migrasi, tetapi perubahan format kanonik itu sendiri tetap membutuhkan migrasi. Hal ini dapat diterima karena format kanonik dikendalikan BNPB, bukan instansi sumber.

### **9.3.5 Isi attributes tidak divalidasi dan tidak di-index** {#9.3.5-isi-attributes-tidak-divalidasi-dan-tidak-di-index}

Kolom attributes menerima tipe nilai apa pun dan belum diberi GIN index. Index dapat ditambahkan kemudian tanpa migrasi data.

## **9.4 Bukti Penyelesaian** {#9.4-bukti-penyelesaian}

### **9.4.1 Restart Independen Container** {#9.4.1-restart-independen-container}

\<placeholder\> 

### **9.4.2 Record Sebelum/Sesudah Schema Change** {#9.4.2-record-sebelum/sesudah-schema-change}

\<placeholder\> 

### **9.4.3 Canonical Store Hanya Diakses Aggregator** {#9.4.3-canonical-store-hanya-diakses-aggregator}

\<placeholder\> 

## **9.5 Batas Service dan Alternatif Penyimpanan** {#9.5-batas-service-dan-alternatif-penyimpanan}

### **9.5.1 Alternatif Batasan Service** {#9.5.1-alternatif-batasan-service}

| Pendekatan | Deskripsi | Hasil |
| ----- | ----- | :---: |
| Role Based | Satu service per tanggung jawab fungsional: Aggregator, Client-Facing API, Auth/Token Service, serta consumer terpisah untuk setiap pihak yang berlangganan event. | Dipilih |
| Monolith | Polling, pemetaan, autentikasi, dan penyajian dalam satu unit deploy. | Ditolak |
| Source-adapter | Adapter BMKG dan adapter PVMBG terpisah, diikuti service mapper inti. | Ditolak |
| Pemisahan ingest dan read | Aggregator dipecah menjadi service ingest (polling dan pemetaan) dan service read (melayani query). | Ditolak |
| Event sourcing / CQRS | HazardEvent disimpan sebagai log append-only, dengan service read-side yang membentuk proyeksi. | Ditolak |

Monolith (B) ditolak karena perubahan pada satu bagian, misalnya autentikasi, memaksa seluruh sistem di-deploy ulang, termasuk bagian yang tidak berubah. Ini persis masalah yang diangkat [Problem 4\.](#bab-ix-problem-4---arsitektur-microservice-&-flexible-storage)

Adapter per sumber (C) memberikan isolasi kegagalan antar-sumber, tetapi isolasi tersebut sudah diperoleh di dalam Aggregator melalui poller independen dan circuit breaker per sumber ([Problem 2](#bab-vii-problem-2---concurrency,-availability,-&-graceful-degradation)). Pendekatan ini hanya menambah network hop dan jumlah service tanpa manfaat tambahan pada cakupan dua sumber.

Pemisahan ingest dan read (D) ditolak karena kedua service akan membutuhkan akses langsung ke Canonical Store, sehingga melanggar ketentuan bahwa setiap storage dimiliki tepat satu service. Alasan inilah yang membuat ingest dan penyajian data tetap berada dalam satu batas service, yaitu Aggregator.

Event sourcing / CQRS (E) ditolak karena menambah kompleksitas pemeliharaan log dan proyeksi, serta memperkenalkan jeda konsistensi antara keduanya, tanpa kebutuhannya untuk sekarang. Kebutuhan distribusi event sudah dipenuhi oleh message broker pada [Problem 5](#bab-x-problem-5---fan-out-event-distribution/pub-sub).

### **9.5.2 Alternatif Penyimpanan Canonical Store** {#9.5.2-alternatif-penyimpanan-canonical-store}

| Aspek | PostgreSQL \+ JSONB (dipilih) | Relasional kolom tetap | Document store (MongoDB) | Key-value store (Redis) |
| ----- | ----- | ----- | ----- | ----- |
| Field baru (confidence\_level) | Diserap kolom JSONB attributes, tanpa migrasi | Wajib migrasi (ALTER TABLE) dan deploy bersama | Diserap secara native, tanpa migrasi | Diserap secara native, tanpa migrasi |
| Validasi field kanonik (severity, hazard\_type) | Bawaan definisi tabel melalui ENUM, CHECK, dan NOT NULL | Bawaan definisi tabel | Tersedia melalui json schema, tetapi opsional dan dikonfigurasi terpisah | Tidak tersedia, sepenuhnya di level aplikasi |
| Deduplikasi (source, source\_ref\_id) | UNIQUE constraint bawaan | UNIQUE constraint bawaan | Unique index | Hanya melalui desain key, tanpa jaminan atomik lintas operasi |
| Query field kanonik (hazard\_type, severity, occurred\_at) | B-tree index bawaan | B-tree index bawaan | Secondary index didukung | Tidak didukung secara native, index harus dibangun dan dirawat manual |
| Query berdasarkan isi attributes | Dapat ditambahkan melalui GIN index tanpa migrasi data | Tidak berlaku (tidak ada kolom fleksibel) | Didukung secara native | Tidak didukung, harus membaca setiap value |

PostgreSQL dengan JSONB dipilih karena merupakan satu-satunya opsi yang sekaligus menyerap field baru tanpa migrasi dan menegakkan validasi serta deduplikasi field kanonik sebagai bagian bawaan dari definisi tabel. Relasional dengan kolom tetap memberikan validasi yang sama, tetapi setiap field baru dari sumber mewajibkan migrasi, sehingga coupling dari [Problem 1](#bab-vi-problem-1---arsitektur-&-interoperabilitas-data) muncul kembali melalui jalur storage. Document store memenuhi kebutuhan tanpa migrasi dan dapat melakukan validasi melalui *jsonSchema*, tetapi validasi tersebut bersifat opsional dan terpisah dari penyimpanan data, sedangkan fleksibilitas skema penuhnya tidak dibutuhkan karena hanya attributes yang berubah-ubah. Key-value store paling sederhana, tetapi tidak mendukung query berdasarkan field selain key, padahal Client-Facing API membutuhkan filter berdasarkan hazard\_type dan waktu.

Konsekuensi dari pilihan ini adalah dua model data dalam satu tabel, field kanonik tervalidasi ketat, sedangkan isi attributes tidak tervalidasi di tingkat database (lihat [9.3](#9.3-batasan-yang-disadari)).

# **BAB X** **Problem 5 \- Fan-out Event Distribution/Pub-sub** {#bab-x-problem-5---fan-out-event-distribution/pub-sub}

## **10.1 Rancangan Solusi** {#10.1-rancangan-solusi}

Aggregator mempublikasikan HazardEvent ke RabbitMQ melalui satu fanout exchange. Hanya HazardEvent yang baru atau berubah berdasarkan hasil upsert ke Canonical Store yang dipublikasikan, sehingga data yang diterima ulang akibat polling berulang tidak dipublikasikan kembali. Correlation ID dibawa melalui properti AMQP correlation\_id, terpisah dari payload HazardEvent. 

Setiap consumer (Dashboard Updater, Notifier, dan Audit Logger) mendeklarasikan durable queue miliknya sendiri dan mengikatnya ke exchange tersebut. Tidak ada queue yang dibagi antar-consumer, sehingga setiap consumer menerima salinan dari setiap pesan. Aggregator hanya mengenal nama exchange dan tidak mengetahui keberadaan consumer. Audit Logger digunakan untuk mendemonstrasikan penambahan consumer baru tanpa perubahan kode producer.

Di sisi producer, publish() memasukkan pesan ke buffer in-memory berukuran terbatas dan langsung mengembalikan kendali, sehingga loop polling Aggregator tidak menunggu broker. Task latar belakang mengirim pesan dengan publisher confirms dan melakukan retry saat gagal. Jika buffer penuh akibat outage broker yang berkepanjangan, pesan tertua dibuang dan dicatat ke log beserta hazard\_id-nya.

Queue dideklarasikan sebagai durable dan pesan dipublikasikan sebagai persistent (delivery\_mode \= 2), sehingga pesan yang terbit saat consumer mati tetap tersimpan di queue dan diproses setelah consumer menyala kembali.

Seluruh consumer menggunakan manual acknowledgment: pesan baru di-ack setelah selesai diproses, sehingga pesan yang sedang diproses saat consumer crash akan dikirim ulang. Karena redelivery dapat menghasilkan duplikat, setiap consumer dibuat idempoten: \- Dashboard Updater melakukan upsert ke state map in-memory berkunci hazard\_id, sehingga memproses pesan yang sama dua kali menghasilkan state yang sama. \- Notifier dan Audit Logger mencatat pasangan (hazard\_id, severity) yang sudah diproses, dan melewati pesan dengan pasangan yang sudah tercatat. Penggunaan severity dalam kunci memastikan eskalasi tingkat ancaman pada event yang sama, misalnya dari SIAGA menjadi AWAS, tetap diproses.

Catatan (hazard\_id, severity) disimpan pada berkas di disk milik masing-masing consumer. Agar pemeriksaan tidak dibebani latensi I/O, isi berkas dimuat ke memori setiap kali service dinyalakan, dan pemeriksaan dilakukan terhadap data di memori. Setelah pesan berhasil diproses, catatan ditambahkan ke memori sekaligus ditulis ke disk, baru kemudian pesan di-ack. Dengan demikian, catatan tetap ada setelah crash dan restart.

## **10.2 Alasan Keputusan** {#10.2-alasan-keputusan}

### **10.2.1 Pemilihan Broker** {#10.2.1-pemilihan-broker}

RabbitMQ, Kafka, and NATS were each evaluated against the five Problem 5 criteria and against the team's operational constraints under a fixed deadline.

| Aspek | RabbitMQ (dipilih) | Kafka | NATS (core) |
| ----- | ----- | ----- | ----- |
| Fan-out ke consumer independen | Fanout exchange bawaan | Consumer group terpisah per consumer | Subscription terpisah |
| Catch-up setelah consumer restart | Durable queue dan persistent message | Offset consumer group | Tidak didukung tanpa JetStream |
| History event untuk consumer baru | Tidak ada | Ada, sesuai log retension | Tidak ada tanpa JetStream |
| Kebutuhan Spesifikasi (M1) | Satu container, ringan | Lebih berat (KRaft, Partition) | Paling ringan, sampai JetSream diaktifkan |

RabbitMQ dipilih karena mendukung at-least-once secara bawaan (durable queue, persistent message, publisher confirms) dan fanout exchange-nya sesuai langsung dengan kebutuhan distribusi ke banyak consumer. Keunggulan Kafka dalam menyediakan history event bagi consumer baru tidak dibutuhkan pada M1, karena history HazardEvent sudah tersedia melalui API Aggregator.

### **10.2.2 Jenis Exchange** {#10.2.2-jenis-exchange}

Topic exchange dengan pola catch-all, berperilaku sama dengan fanout pada kebutuhan saat ini, tetapi menambah beban merancang skema routing key yang belum terpakai. Fanout dipilih karena lebih sederhana, dan peralihan ke topic exchange tetap dapat dilakukan kemudian.

### **10.2.3 Queue per Consumer** {#10.2.3-queue-per-consumer}

Consumer yang berbagi satu queue akan menerima pesan secara bergiliran, sehingga setiap pesan hanya sampai ke satu consumer. Queue per consumer memastikan setiap consumer menerima setiap event.

### **10.2.4 Kebijakan Buffer Producer** {#10.2.4-kebijakan-buffer-producer}

Buffer tanpa batas berisiko menghabiskan memori saat outage broker, sedangkan buffer yang memblokir membuat producer tertahan. Buffer terbatas yang membuang pesan tertua dipilih karena event terbaru lebih relevan secara operasional, dan event yang dibuang tetap tersimpan di Canonical Store.

### **10.2.5 Manual Acknowledgement** {#10.2.5-manual-acknowledgement}

Automatic acknowledgment menyebabkan pesan hilang bila consumer crash di tengah pemrosesan. Manual acknowledgment dipilih karena kehilangan HazardEvent lebih berbahaya daripada duplikat, dengan konsekuensi setiap consumer wajib idempoten. 

### **10.2.6 Kunci Deduplikasi** {#10.2.6-kunci-deduplikasi}

Kunci hazard\_id saja membuat eskalasi severity pada event yang sama, misalnya SIAGA menjadi AWAS, dianggap duplikat dan tidak diproses. Kunci (hazard\_id, severity) menangani kasus ini tanpa menambah field pada format kanonik.

### **10.2.7 Penyimpanan Status Deduplikasi** {#10.2.7-penyimpanan-status-deduplikasi}

Pemeriksaan langsung ke disk menambah latensi I/O, sedangkan penyimpanan hanya di memori hilang saat crash, yang justru memicu redelivery. Karena itu, catatan dimuat dari disk ke memori saat service dinyalakan, diperiksa di memori, dan ditulis ke keduanya sebelum pesan di-ack.

## **10.3 Batasan Yang Disadari** {#10.3-batasan-yang-disadari}

1. Event dapat tidak terpublikasi saat broker atau Aggregator bermasalah Jika outage broker melebihi kapasitas buffer, pesan tertua dibuang. Buffer juga berada di memori, sehingga pesan yang belum terkirim hilang bila Aggregator crash. Event tersebut tetap tersimpan di Canonical Store, tetapi tidak dipublikasikan ulang karena pola outbox tidak diterapkan.   
2. Consumer baru tidak menerima riwayat event Queue dideklarasikan oleh consumer saat pertama kali dijalankan, sehingga event yang terbit sebelum itu tidak diterima. Riwayat HazardEvent tetap dapat diperoleh melalui API Aggregator.  
3. Queue consumer yang mati dapat tumbuh tanpa batas Selama consumer mati, pesan terus menumpuk di queue-nya karena batas panjang queue maupun TTL pesan tidak dikonfigurasi.  
4. Duplikat masih mungkin terjadi Jika consumer crash setelah memproses pesan tetapi sebelum catatan deduplikasi tersimpan, pesan tersebut akan diproses ulang setelah redelivery, misalnya peringatan terkirim dua kali. Konsekuensi ini diterima karena peringatan ganda lebih aman daripada peringatan yang terlewat.  
5. State Dashboard Updater hilang saat restart State map disimpan di memori dan hanya terbentuk kembali dari event yang diterima setelah restart. Sinkronisasi ulang dari API Aggregator saat start tidak diimplementasikan.   
6. Kunci deduplikasi tidak menangani severity yang naik kembali Dengan kunci (hazard\_id, severity), jika severity sebuah event turun lalu naik kembali ke tingkat yang pernah diproses, misalnya SIAGA menjadi WASPADA lalu kembali SIAGA, kenaikan kedua dianggap duplikat. Pola perubahan ini dinilai jarang terjadi pada cakupan M1.   
7. Catatan deduplikasi tidak pernah dibersihkan Berkas catatan (hazard\_id, severity) terus bertambah dan dimuat seluruhnya ke memori setiap kali service dinyalakan. Pada skala M1 ukurannya tidak signifikan.   
8. RabbitMQ berjalan sebagai satu instance Broker berjalan tanpa clustering, sehingga selama broker mati distribusi event terhenti. Pesan yang sudah berada di queue tetap aman karena bersifat persistent dan tersimpan pada Docker volume.

## **10.4 Bukti Penyelesaian** {#10.4-bukti-penyelesaian}

### **10.4.1 Publish ke Broker** {#10.4.1-publish-ke-broker}

\<placeholder\> 

### **10.4.2 Dua Consumer Independen** {#10.4.2-dua-consumer-independen}

\<placeholder\> 

### **10.4.3 Case Consumer Mati** {#10.4.3-case-consumer-mati}

\<placeholder\> 

### **10.4.4 Penambahan Consumer Baru Tanpa Ubah Producer** {#10.4.4-penambahan-consumer-baru-tanpa-ubah-producer}

\<placeholder\> 

## **10.5 Broker, Pola, dan Semantik Pengiriman** {#10.5-broker,-pola,-dan-semantik-pengiriman}

### **10.5.1 Broker dan Pola** {#10.5.1-broker-dan-pola}

Sistem menggunakan RabbitMQ dengan pola publish-subscribe berbasis fanout exchange. Setiap consumer memiliki durable queue sendiri yang terikat ke exchange, sehingga setiap consumer menerima setiap HazardEvent secara independen dari consumer lain.

### **10.5.2 Semantik Pengiriman** {#10.5.2-semantik-pengiriman}

Semantik pengiriman dibedakan untuk dua jalur: 

* Producer ke broker: at-least-once melalui publisher confirms dan retry. Semantik ini turun menjadi at-most-once hanya bila outage broker berlangsung melebihi kapasitas buffer, karena pesan tertua dibuang dan dicatat ke log.   
* Broker ke consumer: at-least-once untuk seluruh consumer melalui manual acknowledgment. Pesan yang sedang diproses saat consumer crash akan dikirim ulang, dan pesan yang terbit saat consumer mati tetap tertampung di durable queue hingga consumer menyala kembali.

### **10.5.3 Konsekuensi bagi Consumer** {#10.5.3-konsekuensi-bagi-consumer}

Karena pengiriman bersifat at-least-once, consumer dapat menerima pesan yang sama lebih dari sekali, sehingga setiap consumer wajib idempoten: 

* Dashboard Updater: upsert ke state map berkunci hazard\_id, sehingga pesan duplikat menghasilkan state yang sama.   
* Notifier dan Audit Logger: melewati pesan dengan pasangan (hazard\_id, severity) yang sudah tercatat. Catatan disimpan di disk dan dimuat ke memori saat service dinyalakan, sehingga tetap berlaku setelah crash dan restart.

Duplikat masih dapat terjadi bila consumer crash setelah memproses pesan tetapi sebelum catatannya tersimpan. Konsekuensi ini diterima karena peringatan ganda lebih aman daripada peringatan yang terlewat.

# **Deklarasi Penggunaan LLM** {#deklarasi-penggunaan-llm}

### **10.4.3 Subbab** {#10.4.3-subbab}

\<placeholder\> 

### **10.4.4 Subbab** {#10.4.4-subbab}

\<placeholder\> 

# **REFERENSI** {#referensi}

\[1\] The VitalVas, "JWT vs Opaque Access Tokens," Blog Notes, 23-Jun-2026. \[Online\]. Available: [https://blog.vitalvas.com/post/2026/06/23/jwt-vs-opaque-access-tokens/](https://blog.vitalvas.com/post/2026/06/23/jwt-vs-opaque-access-tokens/) \[Accessed: 28-Sep-2026\].

\[2\] PT Cipta Piranti Sejahtera, "Otentikasi dan Otorisasi OAuth," Dokumentasi Integrasi API Accurate Online, 11-Jun-2026. \[Online\]. Available: [https://accurate.id/api-integration/oauth/](https://accurate.id/api-integration/oauth/) \[Accessed: 28-Sep-2026\].
