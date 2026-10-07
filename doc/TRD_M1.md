# Technical Requirements Document — Milestone 1

**Sistem:** Platform Koordinasi Kebencanaan (BMKG–PVMBG–BNPB)
**Kelompok:** Ingress
**Cakupan:** Sistem Terdistribusi Dasar (M1) — dua sumber (BMKG, PVMBG), dua jenis bencana (seismik, vulkanik), BNPB sebagai node koordinasi.
**Bahasa implementasi:** Go, satu module per service, masing-masing dikemas sebagai container Docker terpisah dengan Dockerfile sendiri, dinyalakan lewat satu `docker compose up`.
**Sumber:** `IF4031 - Tugas Besar - M1.md` (spesifikasi resmi), `IF4031_M1_Ingress.md` (laporan kerja kelompok, per 2026-09-30). Trade-off dan alternatif yang dipertimbangkan ada di laporan (Bab VI–X), bukan di dokumen ini.

---

## 0. Peta Koneksi Antar Service

| Service | Terhubung ke | Arah | Protokol |
|---|---|---|---|
| Aggregator | BMKG mock | keluar | REST, header `X-BMKG-Key` |
| Aggregator | PVMBG mock | keluar | REST, header `Authorization: Bearer <oauth-token>` |
| Aggregator | Canonical Store (PostgreSQL) | keluar, pemilik tunggal | koneksi database langsung |
| Aggregator | RabbitMQ | keluar (producer) | AMQP publish, fanout exchange |
| Client-Facing API | Aggregator | keluar | gRPC, internal only |
| Client-Facing API | Auth/Token Service | keluar, hanya saat login/refresh | gRPC, internal only |
| Auth/Token Service | User DB (PostgreSQL terpisah) | keluar, pemilik tunggal | koneksi database langsung |
| Dashboard Updater | RabbitMQ | masuk (consumer) | AMQP subscribe, queue sendiri |
| Notifier | RabbitMQ | masuk (consumer) | AMQP subscribe, queue sendiri |
| Audit Logger | RabbitMQ | masuk (consumer) | AMQP subscribe, queue sendiri |
| Semua service Go | Jaeger | keluar | OTLP gRPC, span trace (§13), tidak pernah memengaruhi request |
| Web Frontend (CSR) | Client-Facing API | keluar | REST/JSON via HTTPS, cookie JWT |
| Media / Tim Lapangan / BNPB Ops (pengguna, via browser) | Web Frontend (CSR) | masuk | HTTPS, browser |

### 0.0 Protokol per Boundary

- BMKG/PVMBG ↔ Aggregator: REST (fixed oleh kontrak spesifikasi, §1–§2).
- Client eksternal ↔ Client-Facing API: REST/JSON.
- Client-Facing API ↔ Aggregator, Client-Facing API ↔ Auth/Token Service: gRPC.
- Identitas/role client dan correlation ID diteruskan sebagai gRPC metadata pada call internal, bukan sebagai field body.
- Aggregator dan Auth/Token Service masing-masing menyediakan health check REST terpisah dari port gRPC-nya (§0.2), untuk dipakai `docker-compose healthcheck`.

**Aturan wajib lintas semua service:**
- Komunikasi antar service: jaringan saja (REST/gRPC/AMQP). Tidak ada pemanggilan fungsi langsung atau shared library business-logic antar module.
- Storage: kepemilikan tunggal per service. Canonical Store hanya boleh diakses Aggregator, ditegakkan lewat pemisahan Docker network.
- Setiap service: Dockerfile sendiri, dapat start/stop independen tanpa mematikan atau me-restart service lain.
- Kredensial/secrets: env var, tidak ter-commit. `.env.example` di repo root mendaftar secrets dan nilai yang boleh diubah, dengan nama yang diberi awalan per service. Wiring internal antar container (URL, port, path) diset di `docker-compose.yml`. Tabel pemetaan nama ada di §10.1.

### 0.1 Correlation ID (wajib, lintas semua service)

Dua titik pembangkitan:

1. **Per request client**: dibangkitkan di Client-Facing API saat request masuk (header `X-Correlation-ID` dari client dipakai bila ada, generate baru bila tidak ada). Diteruskan ke Aggregator dan Auth Service lewat gRPC metadata (`x-correlation-id`), dikembalikan di response header ke client.
2. **Per siklus polling**: dibangkitkan di Aggregator setiap kali satu siklus polling terhadap satu sumber dijalankan — satu ID per pemanggilan poll. Mengikat log permintaan ke sumber, hasil mapping, dan upsert ke Canonical Store dari siklus tersebut.

Kedua jenis correlation ID adalah namespace terpisah, disambungkan hanya lewat `source_ref_id`/`hazard_id` di level log.

### 0.2 Health Check (wajib per service)

`GET /health` di setiap service. Isi minimal: `{"status": "ok"|"degraded"|"down"}`. Kedalaman check per service dirinci di tiap bagian service. Service yang bergantung ke service lain melaporkan `degraded` (bukan `down`) bila dependensi tidak terjangkau tapi service itu sendiri masih bisa melayani sebagian fungsi.

### 0.3 Logging (wajib per service)

Structured logging (JSON, satu baris per event), field minimal: `timestamp`, `level`, `service`, `correlation_id`, `message`.
- Setiap panggilan keluar: `target`, `duration_ms`, `status` (sukses/gagal/timeout).
- Error: detail penyebab.
- Perubahan state: circuit breaker CLOSED→OPEN→HALF-OPEN, outage toggle, schema-version toggle, token refresh, consumer ack/nack.

---

## 1. BMKG (mock)

**Fungsi:** Simulasi instansi sumber seismik. Pasif — hanya merespons permintaan, tidak push ke Aggregator.

**Terhubung ke:** tidak ada dependensi keluar. Diakses oleh: Aggregator.

**Dimiliki:** dataset seed in-memory/file untuk `SeismicEvent` dan `TsunamiWarning`, minimal 20 record saat start.

**Autentikasi:** header `X-BMKG-Key: <api-key>`, static API key, divalidasi setiap request. Permintaan tanpa `X-BMKG-Key`, atau dengan key milik instansi lain (PVMBG), ditolak `401`/`403`.

**Karakteristik non-fungsional:** delay tetap 50–150 ms per request. Tidak ada mekanisme outage untuk BMKG di M1.

**Endpoints:**

| Method | Path | Query | Response |
|---|---|---|---|
| GET | `/seismic-events` | `since=<ISO8601>` | `SeismicEvent[]` |
| GET | `/tsunami-warnings` | `since=<ISO8601>` | `TsunamiWarning[]` (hanya event dgn `potential_tsunami=true`; korelasi ke `SeismicEvent` via `related_event_id` dilakukan di Aggregator) |
| GET | `/health` | — | `{"status":"ok"}` |

**SeismicEvent**
```
event_id            string
magnitude           float
depth_km            float
epicenter_lat       float
epicenter_lon       float
region_name         string
occurred_at         datetime (ISO 8601)
potential_tsunami   bool
```

**TsunamiWarning**
```
warning_id           string
related_event_id     string      # FK -> SeismicEvent.event_id
threat_level         enum(Waspada, Siaga, Awas)
affected_zones       []string
estimated_arrival    datetime
```

### 1.1 Generator Data Baru

| Field | Spesifikasi |
|---|---|
| Mekanisme | Task background terpisah dari request handler, berjalan sejak start, independen dari kapan Aggregator polling |
| Interval | Setiap `EVENT_INTERVAL_S` detik, satu `SeismicEvent` baru ditambahkan ke dataset in-memory (field diacak dalam rentang wajar: `magnitude`, `depth_km`, koordinat, `region_name` dari daftar tetap) |
| TsunamiWarning | Dibangkitkan hanya untuk `SeismicEvent` yang di-generate dengan `potential_tsunami=true`, ditambahkan ke dataset `TsunamiWarning` pada saat yang sama dengan event induknya |
| Concurrency | Akses baca (`GET /seismic-events`) dan tulis (generator) ke dataset in-memory yang sama wajib disinkronkan (mis. lock baca-tulis), agar polling yang datang bersamaan dengan penambahan data tidak membaca state tak-konsisten |
| Efek ke polling | Event baru langsung tersedia untuk permintaan `since=<timestamp>` berikutnya dari Aggregator — tidak ada endpoint terpisah untuk memicu penambahan data, murni berjalan sendiri di background |

**Health check:** liveness saja (`{"status":"ok"}`).

**Logging:** setiap request masuk (endpoint, correlation ID diterima dari Aggregator, hasil validasi `X-BMKG-Key` termasuk penolakan), delay yang disuntikkan, momen seed data dibangkitkan saat start, setiap event baru yang di-generate oleh background task (`event_id`, `region_name`).

**Konfigurasi:** `PORT=8081`, `BMKG_API_KEY`, `SEED_COUNT=20`, `EVENT_INTERVAL_S=10`.

---

## 2. PVMBG (mock)

**Fungsi:** Simulasi instansi sumber vulkanik. Latensi tinggi dan fluktuatif, dengan mekanisme outage instan dan schema-evolution yang dipicu langsung.

**Terhubung ke:** tidak ada dependensi keluar. Diakses oleh: Aggregator, sebagai OAuth client terdaftar.

**Dimiliki:** dataset seed `VolcanicReport` (≥20 record), flag internal `schema_version` (legacy/extended — mengontrol kemunculan `confidence_level`), flag `outage_active`, token store OAuth internal (`access_token`, `refresh_token`, terkait `client_id`).

**Autentikasi — OAuth 2.0, di-host oleh PVMBG mock sendiri:**

| Endpoint | Method | Header | Body | Response |
|---|---|---|---|---|
| `/oauth/token` (initial) | POST | `Authorization: Basic base64(client_id:client_secret)` | `code=<one_time_code>`, `grant_type=authorization_code` | `{access_token, refresh_token, expires_in: 60}` |
| `/oauth/token` (refresh) | POST | sama | `grant_type=refresh_token`, `refresh_token=<refresh_token>` | `{access_token, refresh_token, expires_in: 60}` (refresh token dirotasi) |

`<one_time_code>` untuk `grant_type=authorization_code` adalah nilai tetap dari `PVMBG_ONE_TIME_CODE` (hardcoded via env, bukan digenerate dinamis lewat web interface — cukup untuk kebutuhan mock). PVMBG hanya menerima kode ini **satu kali**; percobaan kedua dengan kode yang sama ditolak `400`/`401`. Setelah dipakai sekali, seluruh perolehan token berikutnya wajib lewat `grant_type=refresh_token`.

Access token dipakai sebagai `Authorization: Bearer <access_token>` untuk mengakses `/volcanic-reports`. Satu client hanya punya satu access token valid pada satu waktu.

**Karakteristik non-fungsional:** delay acak 500 ms–3 detik per request, configurable via env var. `POST /admin/outage` men-toggle instan — selama aktif, PVMBG tidak merespons sama sekali atau langsung mengembalikan error (bukan delay panjang). Durasi dikendalikan penuh oleh operator demo.

**Endpoints:**

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/volcanic-reports` | `?since=<ISO8601>` | `VolcanicReport[]` |
| POST | `/admin/schema-version` | header `X-Admin-Key` | toggle kemunculan `confidence_level`, tanpa restart mock maupun Aggregator |
| POST | `/admin/outage` | header `X-Admin-Key` | toggle outage instan |
| GET | `/health` | — | `{"status":"ok"}` |

Endpoint `/admin/*` dilindungi header `X-Admin-Key: <ADMIN_KEY>` (env, wajib diset; di `.env.example` bernama `PVMBG_ADMIN_KEY`): header tidak ada → `401`, salah → `403`. Pola yang sama dengan `X-BMKG-Key` pada BMKG mock; ditambahkan agar saklar demo tidak bisa dipicu oleh pihak lain.

**VolcanicReport**
```
report_id             string
volcano_id            string
alert_level           enum(Normal, Waspada, Siaga, Awas)
eruption_count_24h    int
ash_column_height_m   float
reported_at           datetime (ISO 8601)
confidence_level      float (0-1)   # muncul hanya setelah /admin/schema-version dipanggil
```

Urutan wajib yang bisa didemokan: sistem dinyalakan → gateway bekerja normal dengan skema lama → `POST /admin/schema-version` dipicu → Aggregator tetap bekerja tanpa restart.

### 2.1 Generator Data Baru

| Field | Spesifikasi |
|---|---|
| Mekanisme | Task background terpisah dari request handler, berjalan sejak start, independen dari kapan Aggregator polling |
| Interval | Setiap `EVENT_INTERVAL_S` detik, satu `VolcanicReport` baru ditambahkan ke dataset in-memory (field diacak dalam rentang wajar: `alert_level`, `eruption_count_24h`, `ash_column_height_m`; `volcano_id` dari daftar tetap; `confidence_level` disertakan hanya bila `schema_version` sedang extended) |
| Kondisi outage | Generator tetap berjalan menambah data selama `outage_active=true` — outage hanya memengaruhi respons `/volcanic-reports`, bukan akumulasi data, sehingga saat outage berakhir Aggregator langsung mendapat backlog data yang terlewat |
| Concurrency | Akses baca (`GET /volcanic-reports`) dan tulis (generator) ke dataset in-memory yang sama wajib disinkronkan, agar polling yang datang bersamaan dengan penambahan data tidak membaca state tak-konsisten |
| Efek ke polling | Report baru langsung tersedia untuk permintaan `since=<timestamp>` berikutnya — tidak ada endpoint terpisah untuk memicu penambahan data |

**Health check:** liveness plus flag `outage_active` saat ini. Perilaku `/health` selama outage aktif (mati total, atau tetap hidup dengan `outage_active: true`) harus konsisten dengan perilaku `/volcanic-reports` saat outage.

**Logging:** setiap request masuk (delay yang disuntikkan, hasil validasi Bearer token termasuk penolakan), setiap pemanggilan `/oauth/token` (client_id, jenis grant, hasil), setiap toggle `/admin/outage`/`/admin/schema-version` (state sebelum/sesudah, timestamp), setiap report baru yang di-generate oleh background task (`report_id`, `alert_level`).

**Konfigurasi:** `PORT=8082`, `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `PVMBG_ONE_TIME_CODE`, `ADMIN_KEY`, `PVMBG_DELAY_MIN_MS=500`, `PVMBG_DELAY_MAX_MS=3000`, `SEED_COUNT=20`, `EVENT_INTERVAL_S=10`.

---

## 3. Aggregator

**Fungsi:** Satu-satunya komponen yang berkomunikasi langsung dengan BMKG dan PVMBG. Melakukan polling, memetakan hasil ke `HazardEvent`, pemilik tunggal Canonical Store, mempublikasikan event baru/berubah ke broker. Menyediakan gRPC read API internal untuk Client-Facing API.

**Terhubung ke:**
- BMKG (`GET /seismic-events`, `/tsunami-warnings`) via `X-BMKG-Key`, REST.
- PVMBG (`GET /volcanic-reports`) via OAuth Bearer token, REST. Siklus hidup token dikelola Aggregator — lihat §3.0.
- Canonical Store (PostgreSQL) — satu-satunya service dengan connection string/kredensial DB, pada Docker network internal yang hanya diikuti Aggregator.
- RabbitMQ — publish only, tidak subscribe.
- Diakses oleh: Client-Facing API, lewat gRPC read API internal (tidak diekspos ke luar Docker network).

**Dimiliki:** Canonical Store (§6), buffer in-memory berukuran terbatas untuk outbound publish ke broker, state per sumber untuk circuit breaker & kebaruan data, token OAuth PVMBG saat ini (§3.0).

### 3.0 Token OAuth PVMBG — Bootstrap & Rotasi

**Bootstrap (hanya sekali, saat belum ada token tersimpan):**

| Field | Spesifikasi |
|---|---|
| Kondisi pemicu | `PVMBG_TOKEN_VOLUME_PATH` kosong/belum ada saat Aggregator start (belum pernah bootstrap sebelumnya) |
| Processing | Aggregator memanggil `POST /oauth/token` PVMBG dengan `grant_type=authorization_code`, `code=PVMBG_ONE_TIME_CODE` (nilai tetap dari env, §2), header `Authorization: Basic base64(client_id:client_secret)` |
| Hasil | `access_token`, `refresh_token`, `expires_in` diterima, ditulis ke `PVMBG_TOKEN_VOLUME_PATH` (lihat field yang dipersist di bawah) |
| Setelah bootstrap | `PVMBG_ONE_TIME_CODE` tidak dipakai lagi selama volume token tidak dihapus — seluruh perolehan token berikutnya lewat rotasi refresh token, kecuali pada pemulihan di bawah |

**Rotasi (setiap kali access token mendekati kedaluwarsa):**

| Field | Spesifikasi |
|---|---|
| Kondisi pemicu | Access token akan kedaluwarsa (dihitung dari `expires_at` tersimpan) — rotasi dijadwalkan sebelum `expires_at` tercapai, bukan menunggu poller menerima `401` |
| Processing | Aggregator memanggil `POST /oauth/token` PVMBG dengan `grant_type=refresh_token` dan `refresh_token` yang tersimpan saat ini, header `Authorization: Basic base64(client_id:client_secret)` |
| Hasil | Access token dan refresh token baru diterima, menggantikan token lama di `PVMBG_TOKEN_VOLUME_PATH` sebelum dipakai untuk polling berikutnya |
| Kegagalan rotasi | Refresh token ditolak PVMBG (mis. kedaluwarsa/dicabut) → poller PVMBG mencatat kegagalan, masuk hitungan `consecutive_fail` circuit breaker (§3.3) seperti kegagalan polling biasa |
| Pemulihan (`invalid_grant`) | PVMBG menolak refresh token dengan `invalid_grant` (refresh token tidak dikenalnya lagi, mis. PVMBG restart dan kehilangan token store in-memory beserta catatan bahwa kode sudah dipakai) → Aggregator melakukan bootstrap ulang dengan `PVMBG_ONE_TIME_CODE`, dicatat sebagai event `token bootstrap fallback`. File token lama baru diganti setelah pasangan token baru diterima. Bila bootstrap ulang juga ditolak, kegagalannya diperlakukan seperti kegagalan rotasi di atas dan dicoba lagi pada siklus berikutnya |

**Field yang dipersist di `PVMBG_TOKEN_VOLUME_PATH`:** `access_token`, `refresh_token`, `expires_at` (timestamp absolut, dihitung dari waktu terima + `expires_in` — bukan `expires_in` mentah, agar tidak perlu melacak waktu terima terpisah). Ketiganya ditulis bersamaan setiap kali token diterbitkan/dirotasi, dan dibaca kembali oleh Aggregator setelah restart tanpa mengulang bootstrap.

### 3.1 Model ingest & polling

- Setiap sumber (BMKG, PVMBG) punya poller independen — task terpisah dengan loop, interval, dan timeout masing-masing.
- Single in-flight: setiap poller hanya menjalankan satu panggilan dalam satu waktu. Panggilan berikutnya dimulai setelah panggilan sebelumnya selesai atau timeout. Waktu tunggu sebelum panggilan berikutnya = `POLL_INTERVAL_MS − durasi_panggilan_sebelumnya`; jika durasi melebihi interval, panggilan berikutnya langsung dijalankan.
- Setiap panggilan keluar diberi timeout eksplisit (`POLL_TIMEOUT_MS`); panggilan yang melewati timeout dibatalkan dan dihitung sebagai kegagalan.
- Setiap poller dibungkus circuit breaker tersendiri (§3.3).
- Read API dipisah berdasarkan jenis bahaya — `GetHazards(type: SEISMIC)` hanya membaca data bersumber BMKG, `GetHazards(type: VOLCANIC)` hanya PVMBG.

### 3.2 Pemetaan ke format kanonik

**HazardEvent** (dimiliki & di-generate Aggregator):
```
hazard_id      string    # generated, mis. UUID
source         enum(BMKG, PVMBG)
source_ref_id  string
hazard_type    enum(SEISMIC, VOLCANIC)
severity       enum(NORMAL, WASPADA, SIAGA, AWAS)
area_name      string
latitude       float     # RAW
longitude      float     # RAW
occurred_at    datetime (ISO 8601)
ingested_at    datetime (ISO 8601)
attributes     map[string]any    # RAW, JSONB
```

**Mapping SeismicEvent → HazardEvent:**

| Kanonik | Asal |
|---|---|
| source_ref_id | event_id |
| hazard_type | `SEISMIC` (konstan) |
| area_name | region_name |
| latitude, longitude | epicenter_lat, epicenter_lon |
| occurred_at | occurred_at |
| severity | `AWAS` bila ada TsunamiWarning terkait dengan `threat_level=Awas`; selain itu mengikuti `threat_level` dari warning terkait bila ada; bila tidak ada warning: `magnitude<5.0`→NORMAL, `5.0≤magnitude<6.5`→WASPADA, `magnitude≥6.5`→SIAGA |
| attributes | magnitude, depth_km, potential_tsunami, serta field TsunamiWarning terkait bila ada |

**Mapping VolcanicReport → HazardEvent:**

| Kanonik | Asal |
|---|---|
| source_ref_id | report_id |
| hazard_type | `VOLCANIC` (konstan) |
| area_name | nama gunung api hasil pemetaan dari `volcano_id` (tabel referensi statis milik Aggregator) |
| latitude, longitude | koordinat gunung api dari tabel referensi statis |
| occurred_at | reported_at |
| severity | `alert_level` dipetakan langsung ke enum kanonik |
| attributes | eruption_count_24h, ash_column_height_m, confidence_level (bila tersedia) |

**Kebijakan field tak dikenal (wajib dideklarasikan dan diimplementasikan konsisten):**
- Field baru pada payload sumber yang belum didefinisikan → masuk `attributes`.
- Field kanonik yang didefinisikan tapi tidak ada di JSON yang dikembalikan → bernilai `null`.
- Field yang di-*rename* → field lama bernilai `null`, field baru masuk `attributes`.
- Field yang berubah tipe data → masuk `attributes` dengan key sama.

Perubahan skema yang didukung: penambahan field, pengurangan field, rename field, perubahan tipe data — seluruhnya tanpa restart atau redeploy Aggregator. Tidak didukung: perubahan struktur endpoint (path/method), perubahan makna semantik field kanonik yang sudah dipetakan.

### 3.3 Circuit breaker & status kebaruan data per sumber

State per sumber yang dilacak:

| Field | Keterangan |
|---|---|
| `status` | UP \| DEGRADED \| DOWN |
| `last_success_at` | waktu polling terakhir yang berhasil |
| `stale_since` | waktu data mulai dianggap basi (null bila UP) |
| `consecutive_fail` | jumlah kegagalan polling berturut-turut |

Transisi circuit breaker:
- **CLOSED**: polling normal, status UP.
- **OPEN**: setelah `CB_FAILURE_THRESHOLD` kegagalan berturut-turut — status DOWN, polling ke sumber dihentikan sementara, `stale_since` diisi dengan `last_success_at`.
- **HALF-OPEN**: setelah `CB_OPEN_DURATION_MS`, satu panggilan percobaan dikirim. Berhasil → CLOSED, status UP, `stale_since` dikosongkan, tanpa restart. Gagal → OPEN.

**Perilaku respons berdasarkan kondisi sumber:**

| Kondisi | Respons |
|---|---|
| Sumber UP | 200, data terbaru |
| Sumber DOWN, store berisi data | 200, data terakhir + `meta.stale_since` |
| Sumber DOWN, store kosong | 503, body eksplisit (bukan 500, bukan hang) |
| Permintaan untuk sumber lain | tidak terpengaruh |

Contoh envelope:
```json
{
  "data": [ "...HazardEvent..." ],
  "meta": {
    "source": "PVMBG",
    "source_status": "DOWN",
    "last_success_at": "2026-10-01T10:15:02Z",
    "stale_since": "2026-10-01T10:15:02Z"
  }
}
```

### 3.4 Read API (gRPC, internal)

Dipanggil hanya oleh Client-Facing API. Definisi RPC minimal:

```
rpc GetHazards(GetHazardsRequest) returns (GetHazardsResponse)
rpc GetHealth(google.protobuf.Empty) returns (HealthStatus)

message GetHazardsRequest {
  string hazard_type = 1;   // SEISMIC | VOLCANIC
  int32 limit = 2;          // 0 = default 50, dibatasi 200 di server
  int32 offset = 3;
  string severity = 4;      // opsional: NORMAL | WASPADA | SIAGA | AWAS
  string query = 5;         // opsional: substring area_name, case-insensitive
}
message GetHazardsResponse {
  repeated HazardEvent data = 1;   // satu halaman, terbaru dulu
  ResponseMeta meta = 2;    // source_status, stale_since, last_success_at
  bool has_more = 3;        // masih ada halaman berikutnya
}
```

Identitas/role client dan correlation ID diteruskan sebagai gRPC metadata pada setiap call, bukan sebagai field body.

Pemetaan kondisi-respons §3.3 ke gRPC: `OK` dengan `meta.source_status=DOWN` dan `stale_since` terisi untuk data basi; `UNAVAILABLE` untuk store kosong saat sumber down; `RESOURCE_EXHAUSTED` saat `MAX_INFLIGHT_REQUESTS` terlampaui (diterjemahkan Client-Facing API ke 429 + `Retry-After` untuk client browser).

- Global concurrency cap: semaphore, `MAX_INFLIGHT_REQUESTS`.
- Timeout per hop: Aggregator → Canonical Store punya batas waktu eksplisit; gRPC call dari Client-Facing API dibatasi deadline (`UPSTREAM_TIMEOUT_MS`, §4.3).
- Connection pool tetap: `DB_POOL_SIZE` koneksi ke Canonical Store.

### 3.5 Upsert & publish

- Upsert ke Canonical Store: `INSERT ... ON CONFLICT (source, source_ref_id) DO UPDATE`, dalam transaksi.
- Hanya `HazardEvent` yang baru atau berubah (hasil upsert) yang dipublikasikan.
- Correlation ID dibawa lewat properti AMQP `correlation_id`, terpisah dari payload.
- `publish()` menulis ke buffer in-memory berukuran terbatas dan langsung mengembalikan kendali. Task latar belakang terpisah mengirim pesan dengan publisher confirms dan retry saat gagal. Buffer penuh → pesan tertua dibuang, dicatat ke log beserta `hazard_id`.
- Queue dideklarasikan durable, pesan dipublikasikan sebagai persistent (`delivery_mode=2`).

**Health check:** liveness + status tiap sumber (`BMKG: up/degraded/down`, `PVMBG: up/degraded/down` dengan `last_success_at`/`stale_since`) + konektivitas Canonical Store + konektivitas RabbitMQ. Konektivitas Canonical Store gagal → `status: down`. Satu sumber DOWN tidak membuat `/health` keseluruhan `down`.

**Logging:** setiap siklus polling (correlation ID, sumber, durasi call, hasil sukses/gagal/timeout, jumlah record baru), setiap transisi circuit breaker (dengan `consecutive_fail`), setiap keputusan mapping field tak dikenal yang tereksekusi, setiap request masuk ke read API (correlation ID diterima, tipe hazard diminta), setiap upsert ke Canonical Store (baru/update, `hazard_id`), setiap publish ke broker (sukses/gagal/buffer-drop dengan `hazard_id`).

**Konfigurasi:** `GRPC_PORT`, `HEALTH_PORT` (REST), `POLL_INTERVAL_MS=3000`, `POLL_TIMEOUT_MS=4000`, `CB_FAILURE_THRESHOLD=3`, `CB_OPEN_DURATION_MS=10000`, `MAX_INFLIGHT_REQUESTS=100`, `DB_POOL_SIZE=20`, `DATABASE_URL`, `RABBITMQ_URL`, `BMKG_BASE_URL`, `BMKG_API_KEY`, `PVMBG_BASE_URL`, `PVMBG_OAUTH_CLIENT_ID`, `PVMBG_OAUTH_CLIENT_SECRET`, `PVMBG_ONE_TIME_CODE`, `PVMBG_TOKEN_VOLUME_PATH`.

---

## 4. Client-Facing API

**Fungsi:** Satu-satunya entry point publik (port 8080), diakses client via REST/JSON. Proxy read-only: autentikasi/otorisasi client downstream, menegakkan hak akses per field, rate limiting, transparent JWT refresh. Tidak pernah query Canonical Store langsung.

**Terhubung ke:**
- Aggregator, lewat gRPC (`GetHazards`, §3.4).
- Auth/Token Service, lewat gRPC — hanya untuk login dan refresh.
- Diakses oleh: Media, Tim Lapangan, BNPB Ops, via REST/JSON.

**Dimiliki:** tidak ada storage persisten. State in-memory: token bucket per client (`client_id -> {tokens, last_refill}`), semaphore concurrency cap.

**Endpoints:**

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/auth/login` | body `{email, password}` | Set-Cookie JWT + refresh token (HttpOnly) |
| POST | `/auth/logout` | cookie | invalidasi refresh token |
| POST | `/auth/register` | body `{email, password}`, tanpa auth | Membuat akun baru dengan role tetap `Media`. `409` bila email sudah terdaftar. |
| POST | `/accounts` | body `{email, password, role}`, cookie JWT role `Operator` | Membuat akun baru dengan role `Media`\|`Lapangan`\|`Operator` sesuai body. `403` bila caller bukan Operator. `409` bila email sudah terdaftar. |
| GET | `/hazards?type=SEISMIC\|VOLCANIC` | cookie JWT | Satu halaman `HazardEvent` terfilter sesuai role, terbaru dulu: `{data, meta, page: {limit, offset, has_more}}`. Parameter opsional: `limit` (1-100, default 25), `offset` (≥0), `severity` (`NORMAL`\|`WASPADA`\|`SIAGA`\|`AWAS`), `q` (maks 100 karakter, substring `area_name`, case-insensitive). Nilai di luar rentang `400`. Tidak ada total: `has_more` didapat dengan meminta satu baris ekstra, sehingga tidak ada `COUNT` atas seluruh tabel. |
| GET | `/me` | cookie JWT | `{email, role}` dari klaim JWT yang sudah diverifikasi, agar browser (yang tak bisa membaca cookie HttpOnly) tahu role-nya. `401` tanpa sesi. Hanya petunjuk UI: penegakan role tetap di server. |
| GET | `/health` | — | `{status}` |

**Hak akses per role (ditegakkan server-side):**

| Role | Field yang terlihat |
|---|---|
| Media / Pers | Hanya klasifikasi Ringkasan: hazard_id, source, hazard_type, severity, area_name, occurred_at, ingested_at |
| Tim Lapangan (BASARNAS/Pemda) | Seluruh field (Ringkasan + Mentah: source_ref_id, latitude, longitude, attributes); access token TTL pendek (default 60 detik) |
| BNPB Pusat / Internal Ops | Seluruh field |

Permintaan eksplisit terhadap field mentah oleh client ber-scope Media ditolak `401`/`403`.

### 4.1 `POST /auth/login`

| Field | Spesifikasi |
|---|---|
| Request body | `{email, password}` |
| Processing | Client-Facing API meneruskan `{email, password}` ke Auth/Token Service via RPC `Login`. Auth Service memverifikasi `email` dan `hash(password + secret_salt)` terhadap tabel `users`. |
| Response (sukses) | `Set-Cookie: jwt=<token>; HttpOnly`, `Set-Cookie: refresh_token=<token>; HttpOnly`, body `{expires_in}` |
| Response (gagal) | `401`, tanpa mengindikasikan apakah email atau password yang salah |

**Struktur JWT:**
```
Header:  { "alg": "HS256", "typ": "JWT" }
Payload: { "name", "email", "role": "Media|Lapangan|Operator", "iat", "exp" }
Base64HeaderAndPayload = Base64(Header) + "." + Base64(Payload)
Signature = HMACSHA256(Base64HeaderAndPayload)
Token = Base64HeaderAndPayload + "." + Signature
```
`exp` bergantung role — Tim Lapangan: `iat + 60` detik (default, configurable).

**Verifikasi per request** (semua endpoint bercookie JWT): signature HMAC-SHA256 + klaim `exp` diverifikasi lokal di Client-Facing API, tanpa call ke Auth Service. Valid → request diteruskan ke Aggregator. Invalid dan `exp` kedaluwarsa → lihat §4.2. Invalid karena sebab lain (signature salah) → `401`. Setiap `401` dari verifikasi JWT untuk request yang membawa cookie sesi juga menghapus kedua cookie (`jwt`, `refresh_token`) lewat `Set-Cookie` dengan `Max-Age=0`, karena browser tidak bisa menghapus cookie HttpOnly sendiri; frontend lalu langsung mengarahkan ke `/login`.

### 4.2 Transparent Refresh (dipicu otomatis, bukan endpoint terpisah)

Refresh token bersifat stateful, disimpan di tabel `refresh_tokens` milik Auth Service (skema §5). Di-`Set-Cookie` bersamaan dengan JWT saat login.

| Field | Spesifikasi |
|---|---|
| Kondisi pemicu | Request masuk membawa cookie `jwt` dengan `exp` kedaluwarsa, dan cookie `refresh_token` valid ada |
| Processing | Request asli ditahan di memory. Client-Facing API memanggil RPC `Refresh(refresh_token)` ke Auth Service. Auth Service mencari `refresh_token` di tabel `refresh_tokens` (join `users`); ditemukan → baris dirotasi (refresh token lama diganti baru), JWT baru diterbitkan. Client-Facing API meng-inject JWT baru ke request yang ditahan, meneruskan ke Aggregator. |
| Response (sukses) | Response dari Aggregator diteruskan ke client, disertai `Set-Cookie` JWT dan refresh token baru |
| Response (refresh token tidak ditemukan/invalid) | `401`, cookie JWT dan refresh token dihapus |
| Batasan diketahui | Permintaan paralel dengan refresh token sama dapat saling bertabrakan — hanya satu yang berhasil merotasi; permintaan yang kalah menerima `401` dan cookie JWT/refresh token dihapus (client login ulang, atau mengulang dengan cookie baru bila cookie dari pemenang sudah diterima). Single-flight per refresh token dapat menghindarinya, tetapi belum dibangun di M1 |

### 4.3 Pembatasan konkurensi

- Global concurrency cap: semaphore, `MAX_INFLIGHT_REQUESTS`; penuh → 429 + `Retry-After`.
- Rate limit per client: token bucket, key = `client_id` dari klaim JWT.
- Timeout per hop: setiap panggilan ke Aggregator dibatasi `UPSTREAM_TIMEOUT_MS`.

**Health check:** liveness + konektivitas Aggregator + konektivitas Auth Service. Aggregator tidak terjangkau → `status: degraded`.

**Logging:** correlation ID dibangkitkan untuk setiap request client masuk dan diteruskan ke semua panggilan keluar; setiap request (role, endpoint, field yang diminta, hasil authorize); setiap kejadian rate-limit/429 (client_id, sisa kapasitas); setiap transparent refresh yang terpicu (client_id, hasil rotasi); durasi tiap panggilan ke Aggregator/Auth Service.

**Konfigurasi:** `PORT=8080` (REST publik), `AGGREGATOR_GRPC_ADDR`, `AUTH_SERVICE_GRPC_ADDR`, `JWT_SIGNING_SECRET` (shared dengan Auth Service, internal only), `MAX_INFLIGHT_REQUESTS=100`, `RATE_LIMIT_RPS=50`, `UPSTREAM_TIMEOUT_MS=1000`, `ACCESS_TOKEN_TTL_S=60`, `COOKIE_SECURE=true` (cookie `jwt`/`refresh_token` diberi atribut Secure), `DASHBOARD_UPDATER_URL` (sumber `GET /hazards/stream`; bila kosong endpoint itu tidak ada).

---

## 5. Auth/Token Service (IdP)

**Fungsi:** Identity provider internal BNPB untuk client downstream (Media, Tim Lapangan, Operator) — terpisah dari OAuth server PVMBG (§2). Login, penerbitan JWT bertanda tangan, pengelolaan siklus hidup refresh token.

**Terhubung ke:** User DB (dimiliki sendiri). Diakses oleh: Client-Facing API saja, untuk login dan refresh.

**Dimiliki:** User DB (PostgreSQL terpisah dari Canonical Store).

```sql
CREATE TABLE users (
    user_id       VARCHAR(255) PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role          VARCHAR(32) NOT NULL CHECK (role IN ('Media','Lapangan','Operator'))
);

CREATE TABLE refresh_tokens (
    refresh_token   VARCHAR(64) PRIMARY KEY,
    user_id         VARCHAR(255) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

**RPC (gRPC, internal only):**

```
rpc Login(LoginRequest) returns (TokenResponse)      // {email, password} -> {jwt, refresh_token, expires_in}
rpc Refresh(RefreshRequest) returns (TokenResponse)   // {refresh_token} -> {jwt, refresh_token, expires_in}, rotated
rpc Logout(LogoutRequest) returns (google.protobuf.Empty)  // {refresh_token} -> hapus row
rpc Register(RegisterRequest) returns (google.protobuf.Empty)       // {email, password} -> insert row role=Media
rpc CreateAccount(CreateAccountRequest) returns (google.protobuf.Empty)  // {email, password, role} -> insert row role sesuai body
```

**Lifecycle refresh token:** row `refresh_tokens` dihapus pada dua kondisi — saat refresh (baris lama diganti baris rotasi baru), dan saat logout.

**Pembuatan akun:**

| Role | Dibuat via | Siapa yang bisa memicu |
|---|---|---|
| Media | `POST /auth/register` (Client-Facing API → RPC `Register`) | Siapa saja, tanpa auth |
| Lapangan | `POST /accounts` dengan `role=Lapangan` (Client-Facing API → RPC `CreateAccount`) | Operator saja |
| Operator | `POST /accounts` dengan `role=Operator`, atau `SEED_OPERATOR_EMAIL`/`SEED_OPERATOR_PASSWORD` saat start | Operator saja (selain akun seed awal) |

Pengecekan role caller (`Operator`) untuk `POST /accounts` dilakukan di Client-Facing API dari klaim JWT, bukan di Auth Service — RPC `CreateAccount` mempercayai pemanggilnya (pola yang sama dengan RPC internal lain, §0.0). Tidak ada verifikasi email di M1.

**Health check:** liveness + konektivitas User DB. User DB tidak terjangkau → `status: down`.

**Logging:** setiap login (email, hasil sukses/gagal — tanpa membocorkan alasan gagal spesifik ke client), setiap refresh (user_id, rotasi sukses/gagal), setiap logout, setiap penerbitan JWT (role, `exp` yang dipakai).

**Konfigurasi:** `GRPC_PORT`, `HEALTH_PORT` (REST), `USER_DB_URL`, `JWT_SIGNING_SECRET` (shared dengan Client-Facing API), `SECRET_SALT`, `ACCESS_TOKEN_TTL_S=60`, `SEED_OPERATOR_EMAIL`, `SEED_OPERATOR_PASSWORD`.

---

## 6. Canonical Store (PostgreSQL)

**Fungsi:** Menyimpan `HazardEvent`. Storage dimiliki tunggal oleh Aggregator.

**Dimiliki oleh:** Aggregator saja, pada Docker network internal yang hanya diikuti Aggregator. Service lain tidak menjangkau secara jaringan, harus lewat read API Aggregator.

**Skema — kolom bertipe untuk field kanonik + JSONB untuk atribut dinamis:**

```sql
CREATE TABLE hazard_events (
    hazard_id     UUID PRIMARY KEY,
    source        VARCHAR(16) NOT NULL CHECK (source IN ('BMKG','PVMBG')),
    source_ref_id VARCHAR(255) NOT NULL,
    hazard_type   VARCHAR(16) NOT NULL CHECK (hazard_type IN ('SEISMIC','VOLCANIC')),
    severity      VARCHAR(16) CHECK (severity IN ('NORMAL','WASPADA','SIAGA','AWAS')),
    area_name     TEXT,
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    occurred_at   TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL,
    attributes    JSONB,
    UNIQUE (source, source_ref_id)
);
CREATE INDEX idx_hazard_type_occurred ON hazard_events (hazard_type, occurred_at);
-- dapat ditambahkan kemudian tanpa migrasi data:
-- CREATE INDEX idx_attributes_gin ON hazard_events USING GIN (attributes);
```

NOT NULL hanya pada: `hazard_id`, `source`, `source_ref_id`, `hazard_type`, `occurred_at`, `ingested_at`. `UNIQUE (source, source_ref_id)` mencegah duplikasi record akibat polling berulang.

**Konfigurasi:** `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, Docker volume untuk persistensi lintas restart.

---

## 7. Message Broker (RabbitMQ)

**Fungsi:** Distribusi fan-out `HazardEvent` dari Aggregator ke consumer independen.

**Dimiliki oleh:** dirinya sendiri (queue storage di Docker volume). Aggregator adalah satu-satunya producer.

**Topologi:** satu fanout exchange (`hazard.events`). Setiap consumer mendeklarasikan durable queue miliknya sendiri, diikat ke exchange. Tidak ada queue yang dibagi antar-consumer. Pesan dipublikasikan sebagai persistent (`delivery_mode=2`).

**Semantik pengiriman:**
- Producer → broker: at-least-once (publisher confirms + retry di Aggregator); turun ke at-most-once bila outage broker melebihi kapasitas buffer producer.
- Broker → consumer: at-least-once, manual acknowledgment.

**Health check:** RabbitMQ menyediakan health check bawaan (`rabbitmq-diagnostics check_running`/management API). Aggregator dan tiap consumer mencerminkan konektivitas broker di health check masing-masing.

**Logging:** dari sisi Aggregator (producer) dan tiap consumer.

**Konfigurasi:** `RABBITMQ_URL`, `RABBITMQ_EXCHANGE=hazard.events` (fanout).

---

## 8. Event Consumers (minimal 2, direkomendasikan 3)

Setiap consumer adalah service/container tersendiri, dapat di-deploy independen, berlangganan exchange yang sama. Aggregator hanya mengenal nama exchange, bukan consumer.

### 8.1 Dashboard Updater

**Fungsi:** Update otomatis begitu ada `HazardEvent` baru, untuk dashboard internal.
**Terhubung ke:** RabbitMQ (subscribe only).
**Dimiliki:** state map in-memory `hazard_id -> HazardEvent`.
**Idempotensi:** upsert ke state map berkunci `hazard_id`.
**Ack:** manual, setelah upsert selesai.
**Push realtime:** `GET /stream` (internal, tidak dipublikasikan ke host) adalah Server-Sent Events: satu frame `data:` berisi `HazardEvent` lengkap (JSON) setiap kali upsert selesai, plus komentar `: keep-alive` tiap 15 detik. Setiap klien punya buffer 64 event; klien yang tertinggal (lambat/mati) diputus, tidak pernah memblokir consumer atau klien lain. Dashboard Updater berdiri sendiri: dihentikan tidak mengganggu Aggregator maupun consumer lain, dan klien mendapat 502 pada stream sampai ia hidup lagi.
**Batasan diketahui:** state hilang saat restart, tidak ada resync dari API Aggregator saat start.

### 8.2 Notifier

**Fungsi:** Mengirim peringatan ke perangkat Tim Lapangan begitu ada `HazardEvent` baru/berubah.
**Terhubung ke:** RabbitMQ (subscribe only).
**Dimiliki:** catatan dedup di disk, berkunci `(hazard_id, severity)`.
**Idempotensi:** catatan dimuat ke memori saat start; pemeriksaan dilakukan di memori. Setelah pesan diproses, catatan ditambahkan ke memori dan ditulis ke disk, baru kemudian pesan di-ack.
**Batasan diketahui:** severity yang turun lalu naik kembali ke level yang pernah diproses dianggap duplikat. Duplikat tetap mungkin terjadi jika consumer crash setelah memproses pesan tapi sebelum catatan tersimpan.

### 8.3 Audit Logger (direkomendasikan, kriteria P5.4)

**Fungsi:** Mencatat setiap `HazardEvent` yang diterima; mendemonstrasikan penambahan consumer baru pada topic yang sudah ada tanpa mengubah kode producer.
**Terhubung ke:** RabbitMQ (subscribe only).
**Dimiliki:** log append-only.
**Idempotensi:** skema dedup sama seperti Notifier, `(hazard_id, severity)`.

**Health check (semua consumer):** liveness + status koneksi ke RabbitMQ (`connected`/`disconnected`) + untuk Notifier/Audit Logger, keterbacaan dedup store di disk saat start.

**Logging (semua consumer):** setiap pesan diterima (correlation ID dari properti AMQP, `hazard_id`, `severity`), hasil pemrosesan (diproses/dilewati karena dedup), momen ack dikirim, setiap disconnect/reconnect ke broker. Notifier/Audit Logger: momen catatan dedup ditulis ke disk dicatat terpisah dari momen ack.

**Konfigurasi (per consumer):** `RABBITMQ_URL`, `RABBITMQ_EXCHANGE`, `QUEUE_NAME` (unik per consumer), `DEDUP_STORE_PATH` (Notifier, Audit Logger).

### 8.4 Kebijakan buffer producer

Buffer producer in-memory berukuran terbatas, membuang pesan tertua saat penuh (bukan tanpa batas, bukan memblokir). Event yang dibuang tetap tersimpan di Canonical Store.

### 8.5 Manual acknowledgment

Seluruh consumer menggunakan manual acknowledgment — konsekuensinya, setiap consumer wajib idempoten (§8.1–8.3).

---

## 9. Web Frontend (CSR)

**Fungsi:** Dashboard mock untuk client downstream (Media, Tim Lapangan, BNPB Ops) — login, lihat daftar `HazardEvent`, registrasi akun Media, dan (untuk Operator) pembuatan akun Lapangan/Operator.

**Terhubung ke:** Client-Facing API saja, via REST/JSON (§4). Tidak ada koneksi langsung ke Aggregator, Auth/Token Service, RabbitMQ, atau database mana pun.

**Dimiliki:** tidak ada storage server-side. Hanya berkas statis (HTML/JS/CSS) yang disajikan oleh static file server/container. State di sisi browser (data hasil fetch, form input) tidak persisten lintas reload kecuali lewat mekanisme browser standar (localStorage untuk preferensi UI non-kritis, bukan untuk token — token JWT/refresh tetap di cookie HttpOnly yang dikelola Client-Facing API).

**Mode rendering:** Client-Side Rendering — server hanya menyajikan berkas statis tanpa logika render; seluruh fetching data dan rendering UI berjalan di browser, memanggil endpoint REST Client-Facing API. Cookie HttpOnly disertakan otomatis oleh browser pada setiap call (`credentials: include`), tanpa frontend perlu membaca/menyimpan token secara eksplisit.

**Endpoint Client-Facing API yang dipakai:** `POST /auth/login`, `POST /auth/logout`, `POST /auth/register`, `POST /accounts` (khusus role Operator), `GET /hazards`, `GET /hazards/stream`.

**Update data (push):** frontend membuka `EventSource` ke `GET /hazards/stream?type=` (via Client-Facing API, yang tetap satu-satunya pintu publik). Client-Facing API meneruskan `GET /stream` Dashboard Updater (`DASHBOARD_UPDATER_URL`), menyaring per `type`, dan memproyeksikan field per role seperti `GET /hazards` (Media hanya field ringkasan; Lapangan/Operator semua field). JWT diperiksa sama seperti endpoint lain saat stream dibuka; stream ditutup server saat `exp` token tercapai, sehingga sesi kedaluwarsa tidak bisa mempertahankannya — `EventSource` menyambung ulang otomatis dan middleware me-refresh token secara transparan atau menjawab 401. Stream punya batas jumlah (500) sendiri dan tidak dihitung dalam `MAX_INFLIGHT_REQUESTS`. Terukur pada stack lokal: event muncul di stream ±4 ms setelah `ingested_at` dari Aggregator (event BMKG 2026-10-06, `ingested_at` 15:21:56.6205, diterima 15:21:56.6246).

**Pilihan mekanisme:** SSE, bukan WebSocket — komunikasi hanya satu arah, jalan di atas HTTP biasa melalui nginx dan cookie HttpOnly, dan sambung ulang otomatis sudah ada di browser.

**Klien yang tersambung ulang:** event yang terlewat tidak diputar ulang. Saat stream terbuka (termasuk sambung ulang) frontend memuat ulang `GET /hazards`, yang sumber kebenarannya Canonical Store, jadi daftar kembali lengkap. Tiap event push juga memicu muat ulang itu, sehingga paging, filter, dan pencarian tetap benar. Polling `GET /hazards` pada `FRONTEND_POLL_INTERVAL_MS` tetap berjalan sebagai cadangan bila stream putus.

**Konfigurasi:** `CLIENT_FACING_API_BASE_URL` (di-bake saat build, karena berkas statis), `FRONTEND_POLL_INTERVAL_MS`.

---

## 10. Referensi Konfigurasi Lintas Service

| Variabel | Service pemilik | Default | Keterangan |
|---|---|---|---|
| `POLL_INTERVAL_MS` | Aggregator | 3000 | interval polling per sumber |
| `POLL_TIMEOUT_MS` | Aggregator | 4000 | timeout satu panggilan polling |
| `CB_FAILURE_THRESHOLD` | Aggregator | 3 | kegagalan berturut sebelum breaker OPEN |
| `CB_OPEN_DURATION_MS` | Aggregator | 10000 | durasi OPEN sebelum trial HALF-OPEN |
| `MAX_INFLIGHT_REQUESTS` | Aggregator, Client-Facing API | 100 | concurrency cap (masing-masing punya sendiri) |
| `RATE_LIMIT_RPS` | Client-Facing API | 50 | token bucket per client |
| `UPSTREAM_TIMEOUT_MS` | Client-Facing API | 1000 | timeout memanggil Aggregator |
| `DASHBOARD_UPDATER_URL` | Client-Facing API | - | alamat `GET /stream` Dashboard Updater untuk push realtime |
| `DB_POOL_SIZE` | Aggregator | 20 | connection pool Canonical Store |
| `ACCESS_TOKEN_TTL_S` | Auth Service, Client-Facing API | 60 | TTL JWT |
| `PVMBG_DELAY_MIN_MS` / `_MAX_MS` | PVMBG mock | 500 / 3000 | simulasi latensi |
| `JWT_SIGNING_SECRET` | Auth Service, Client-Facing API | — | shared HMAC secret, internal only |
| `TRACING_ENABLED` | semua service Go | true | ekspor span OpenTelemetry ke Jaeger (§13) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | semua service Go | http://jaeger:4317 | alamat OTLP gRPC tujuan span |
| `JAEGER_UI_PORT` | Jaeger | 16686 | port host untuk UI Jaeger |
| `AUTH_RATE_LIMIT_RPS` | Client-Facing API | 10 | token bucket per IP klien untuk `/auth/login` dan `/auth/register` |

### 10.1 Nama variabel: yang dibaca service vs `.env.example`

Service membaca nama di kolom kedua. `docker-compose.yml` mengisinya dari `.env.example` (kolom ketiga) atau dengan nilai tetap (kolom "compose").

| Service | Dibaca service | Asal |
|---|---|---|
| BMKG mock | `SEED_COUNT`, `EVENT_INTERVAL_S` | `BMKG_SEED_COUNT`, `BMKG_EVENT_INTERVAL_S` |
| BMKG mock | `BMKG_DELAY_MIN_MS`, `BMKG_DELAY_MAX_MS` | sama |
| PVMBG mock | `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET` | `PVMBG_OAUTH_CLIENT_ID`, `PVMBG_OAUTH_CLIENT_SECRET` |
| PVMBG mock | `ADMIN_KEY` | `PVMBG_ADMIN_KEY` |
| PVMBG mock | `SEED_COUNT`, `EVENT_INTERVAL_S` | `PVMBG_SEED_COUNT`, `PVMBG_EVENT_INTERVAL_S` |
| Aggregator | `MAX_INFLIGHT_REQUESTS` | `AGGREGATOR_MAX_INFLIGHT_REQUESTS` |
| Aggregator | `DATABASE_URL` | compose, disusun dari `CANONICAL_DB_USER`, `CANONICAL_DB_PASSWORD`, `CANONICAL_DB_NAME` |
| Aggregator | `RABBITMQ_URL`, `RABBITMQ_EXCHANGE` | compose, `RABBITMQ_EXCHANGE` dari `.env.example` |
| Aggregator | `BMKG_BASE_URL`, `PVMBG_BASE_URL`, `GRPC_PORT`, `HEALTH_PORT`, `PVMBG_TOKEN_VOLUME_PATH` | compose |
| Auth Service | `USER_DB_URL` | compose, disusun dari `USER_DB_USER`, `USER_DB_PASSWORD`, `USER_DB_NAME` |
| Auth Service | `GRPC_PORT`, `HEALTH_PORT` | compose |
| Canonical Store | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `CANONICAL_DB_USER`, `CANONICAL_DB_PASSWORD`, `CANONICAL_DB_NAME` |
| User DB | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `USER_DB_USER`, `USER_DB_PASSWORD`, `USER_DB_NAME` |
| Client-Facing API | `MAX_INFLIGHT_REQUESTS` | `CLIENT_FACING_API_MAX_INFLIGHT_REQUESTS` |
| Client-Facing API | `AGGREGATOR_GRPC_ADDR`, `AUTH_SERVICE_GRPC_ADDR`, `DASHBOARD_UPDATER_URL`, `PORT` | compose |
| Consumer | `RABBITMQ_URL`, `RABBITMQ_EXCHANGE`, `QUEUE_NAME`, `HEALTH_PORT` | compose |
| Notifier, Audit Logger | `DEDUP_STORE_PATH` | compose |
| Audit Logger | `AUDIT_LOG_PATH` | default di kode, tidak diisi compose |
| Web Frontend | `CLIENT_FACING_API_BASE_URL`, `FRONTEND_POLL_INTERVAL_MS` | `.env.example`, di-bake saat build |


---

## 11. Traceability — Problem → Service Pemilik

| Problem | Service pemilik utama | Kriteria terukur |
|---|---|---|
| P1 — Interoperabilitas Data | Aggregator (§3.2), PVMBG mock (§2) | Toggle schema-version tanpa restart Aggregator; HazardEvent valid sebelum & sesudah; kebijakan field tak dikenal konsisten |
| P2 — Concurrency/Availability | Aggregator (§3.1, §3.3, §3.4), Client-Facing API (§4.3) | p95 BMKG-only <300ms saat delay PVMBG 3s; load test ≥50 koneksi/60s, error rate di luar 429 <1%; graceful degradation saat outage PVMBG |
| P3 — Auth & Trust | BMKG mock (§1), PVMBG mock (§2), Client-Facing API (§4.1–4.2), Auth Service (§5) | Kredensial lintas domain ditolak 401/403; scope Media ditolak field mentah; token expiry+refresh terdemonstrasi; tidak ada kredensial hardcoded |
| P4 — Microservice & Storage | Seluruh service (§0), Canonical Store (§6) | Container restart independen; storage fleksibel tanpa migrasi; Canonical Store hanya lewat Aggregator |
| P5 — Fan-out Pub/Sub | Aggregator (§3.5, producer), RabbitMQ (§7), Consumers (§8) | Publish ke broker; ≥2 consumer independen; consumer mati tidak menahan producer; consumer baru ditambah tanpa ubah producer |

---

## 12. Batasan Diketahui (per area)

**Concurrency/Availability (P2):**
- Data bersifat eventually consistent (lag = interval polling + latensi sumber).
- Deteksi outage tidak instan — butuh `CB_FAILURE_THRESHOLD` kegagalan berturut sebelum breaker OPEN.
- Sumber yang sangat lambat diperlakukan sama dengan sumber yang mati.
- Load shedding tidak memprioritaskan client.
- Client-Facing API dan Aggregator: satu instance, state in-memory hilang saat restart.
- Aggregator dan Canonical Store adalah dependensi jalur akses — matinya salah satu membuat seluruh permintaan data gagal (503 eksplisit).
- Hasil load testing bergantung pada lingkungan pengujian.

**Auth & Trust (P3):**
- Permintaan paralel dengan refresh token sama dapat saling bertabrakan karena hanya satu yang berhasil melakukan rotasi.

**Microservice & Storage (P4):**
- Restart Auth/Token Service berdampak pada login dan refresh (503 sementara); permintaan dengan token valid tetap dilayani.
- Restart Aggregator menghentikan sementara ingest dan permintaan data (503 eksplisit); login, refresh, dan consumer tetap berjalan.
- Canonical Store berjalan sebagai satu instance tanpa replikasi.
- Fleksibilitas skema hanya berlaku untuk atribut sumber (lewat `attributes`); perubahan format kanonik tetap butuh migrasi.
- Isi `attributes` tidak divalidasi dan belum di-index.

**Fan-out Pub/Sub (P5):**
- Event dapat tidak terpublikasi bila outage broker melebihi kapasitas buffer producer, atau bila Aggregator crash sebelum buffer terkirim.
- Consumer baru tidak menerima riwayat event.
- Queue consumer yang mati dapat tumbuh tanpa batas.
- Duplikat masih mungkin terjadi bila consumer crash setelah memproses pesan tapi sebelum catatan dedup tersimpan.
- State Dashboard Updater hilang saat restart.
- Kunci dedup `(hazard_id, severity)` tidak menangani severity yang turun lalu naik kembali ke level yang sama.
- Catatan dedup tidak pernah dibersihkan.
- RabbitMQ berjalan sebagai satu instance tanpa clustering.

**Interoperabilitas Data (P1):**
- Field baru, field yang di-rename, dan field yang berubah tipe tidak merusak sistem (masuk ke `attributes`), tetapi field kanonik yang bergantung pada nama atau tipe lama menjadi `null`. Contoh: `magnitude` yang berganti nama membuat severity cadangan kosong, dan `epicenter_lat`/`epicenter_lon`/`region_name`/`alert_level` yang berubah membuat latitude, longitude, area_name atau severity kosong. Nilai aslinya tetap ada di `attributes`, tetapi tidak ada yang membacanya.
- Bila field id atau waktu (`event_id`, `report_id`, `occurred_at`, `reported_at`) berubah, record ditolak dan dihitung sebagai rejected, karena keduanya wajib untuk menyimpan.
- Perubahan makna pada field dengan nama dan tipe yang sama (mis. satuan atau skala) tidak terdeteksi.
- Tabel gunung api (`volcanoRef`) statis: `volcano_id` baru tetap disimpan tetapi tanpa nama dan koordinat sampai tabel diubah. Satu severity terpadu diturunkan dari skala tiap sumber, sehingga nuansa spesifik sumber hanya ada di `attributes`.

**Concurrency/Availability (P2), tambahan:**
- Cap konkurensi bersifat global, bukan per client: satu client yang mengirim body sangat lambat ke `POST /auth/login` memegang satu slot sampai batas baca 10 detik, dan throttle per IP (10 request per detik) cukup longgar untuk menjaga sekitar 100 slot terisi. Selama itu request lain selain `/health` mendapat `429`. Efeknya berakhir beberapa detik setelah client berhenti, dan ini belum diuji.
- Kesegaran data dibatasi interval polling (`POLL_INTERVAL_MS`). Status basi dilaporkan per sumber lewat `stale_since`, bukan per record.
- Angka performa (mis. throughput dan p95) diukur di satu mesin terhadap mock, sehingga menunjukkan desain bertahan pada beban itu, bukan kinerja deployment nyata.

**Auth & Trust (P3), tambahan:**
- Refresh token tidak punya masa berlaku: sah sampai dipakai sekali (rotasi) atau logout. Tidak ada batas jumlah token per user dan baris dari sesi yang ditinggalkan tidak pernah dibersihkan, sehingga tabel `refresh_tokens` hanya tumbuh. Cookie refresh yang bocor tetap berlaku tanpa batas waktu selama belum dipakai.
- `POST /auth/register` menjawab `409` untuk email yang sudah ada dan `201` bila belum, jadi keberadaan akun bisa diuji lewat registrasi. Login tidak membocorkan ini. Menyembunyikannya membutuhkan verifikasi email, di luar M1. Throttle per IP memperlambat tetapi tidak menghentikan.
- Password disimpan sebagai `hex(sha256(password + SECRET_SALT))` sesuai §4.1: satu salt global dan hash cepat. Bila User DB bocor bersama salt, tebakan offline cepat dan dua user dengan password sama punya hash sama. Tidak diperkeras terhadap tebakan offline.
- Contoh secret di `.env.example` (`change-me*`) diterima saat start: tidak ada service yang menolak nilai itu. Deployer wajib menggantinya sebelum sistem diekspos.
- Batas kepercayaan ada di tepi sistem, bukan antar service. JWT hanya diverifikasi Client-Facing API. Ke Aggregator, role dan correlation ID dikirim sebagai gRPC metadata biasa tanpa TLS dan dipercaya, dan jaringan internal Docker dipercaya (bukan zero trust). Secret JWT dipegang dua service, sehingga kompromi salah satunya memungkinkan pemalsuan token. Edge tanpa HTTPS pada demo (cookie `Secure` butuh `COOKIE_SECURE=false` di HTTP biasa). Hak akses kasar: tiga role tetap, read-only, tanpa izin per resource atau wilayah. Kedaluwarsa token dibandingkan dengan satu jam tanpa toleransi.

**Fan-out Pub/Sub (P5), tambahan:**
- Port manajemen RabbitMQ (15672) dipublikasikan ke host dengan user administrator, dan consumer mempercayai setiap pesan di `hazard.events`: siapa pun yang menjangkau port itu dapat menerbitkan `HazardEvent` palsu (mis. severity `AWAS`) yang memicu alert Notifier dan feed langsung tanpa melewati Aggregator. Login yang sama dapat mengosongkan queue atau menghapus exchange. Broker diperlakukan sebagai jaringan tepercaya dan event tidak diautentikasi.
- Pengiriman at-least-once: consumer dapat melihat event yang sama dua kali (ditangani dedup), dan crash antara mengirim alert dan mencatatnya dapat mengirim alert dua kali. Feed langsung tidak durable: client yang terputus kehilangan event dan harus memuat ulang daftar. Canonical Store, event broker, dan feed langsung dapat berbeda sesaat setelah penulisan (eventual consistency).
- Audit log dan file dedup Audit Logger serta file dedup Notifier tumbuh tanpa batas (tanpa rotasi atau pemangkasan) dan seluruh kunci dedup dimuat ke memori saat start. Pada laju data demo ini baru berarti setelah waktu yang sangat lama. Disk penuh membuat Audit Logger gagal menulis dan pesan di-requeue.

**Operasional (lintas problem):**
- Satu instance setiap service di satu host, dengan state di memori proses (rate limiter per akun, rotasi refresh bersama, state circuit breaker, kursor polling Aggregator). Menjalankan dua salinan akan memecah state itu. Setiap komponen adalah single point of failure.
- Migrasi skema hanya berjalan pada volume baru (init directory Postgres). Tidak ada jalur upgrade untuk volume yang sudah ada, tidak ada backup atau kebijakan retensi (event, audit log, dan email user disimpan selamanya), dan tidak ada pengumpulan log terpusat. Tracing dapat dimatikan dan penyimpanan trace tidak persisten.
- UI Jaeger dipublikasikan ke host tanpa login. Trace memuat path dan query string, correlation ID, waktu, dan pesan error, tetapi tidak cookie, token, atau body sepengetahuan kami (atribut belum diperiksa satu per satu).

**Asumsi:**
- BMKG dan PVMBG adalah simulasi sesuai kontrak spesifikasi. Layanan nyata akan berbeda dalam autentikasi, rate limit, waktu respons, dan kestabilan id. Perilaku yang terbukti terhadap mock adalah bukti untuk desain, bukan untuk produksi.
- Aturan pemetaan severity, klasifikasi field Ringkasan/Mentah, dan matriks hak akses tiga client mengikuti spesifikasi apa adanya. Lingkup: dua instansi, dua jenis bencana, BNPB sebagai node koordinasi, client read-only.
- Deployer mengganti setiap nilai `change-me*` sebelum sistem diekspos, dan host yang menjalankan stack adalah jaringan tepercaya (alat observability seperti Jaeger tidak diautentikasi).

---

## 13. Observability

**Keputusan:** tracing terdistribusi dibangun, dashboard metrik dan log terpusat tidak. Health check, log terstruktur dengan correlation ID, dan pencatatan latensi per panggilan keluar (§0.1–§0.3) tetap menjadi dasar.

**Yang dibangun:**
- Setiap service Go mengekspor span OpenTelemetry lewat OTLP gRPC ke Jaeger (container `jaeger`, all-in-one), dengan `service.name` per service.
- Konteks trace dibawa lewat header W3C `traceparent` pada HTTP, otelgrpc pada gRPC, dan header pesan AMQP dari Aggregator ke consumer. Satu siklus polling atau satu request client menjadi satu trace lintas service, dengan satu span per panggilan keluar.
- Bila tracing nyala, trace ID dipakai sebagai `correlation_id` di log, jadi satu ID menghubungkan log dan Jaeger. Bila mati, correlation ID tetap UUID seperti §0.1.
- Kegagalan tracing tidak pernah memengaruhi request: ekspor bersifat batch dan asinkron, dan Jaeger yang mati tidak mengubah status `/health` service mana pun. `TRACING_ENABLED=false` mematikan seluruhnya.
- Tab Monitor Jaeger (laju request, error, latensi per service) dibaca dari container Prometheus yang hanya terjangkau dari jaringan compose. Trace tersimpan di memori Jaeger dan hilang saat restart.

**Konfigurasi:** `TRACING_ENABLED` (default `true`), `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://jaeger:4317`), `JAEGER_UI_PORT` (default `16686`).

**Belum dibangun:** log aggregation terpusat (mis. Loki, ELK) dan dashboard Grafana. Kondisi yang mengubah keputusan ini: jumlah instance per service bertambah (>1), atau volume log tidak lagi bisa ditelusuri manual lewat `docker compose logs`. Sinyal prioritas bila dibangun: status circuit breaker per sumber, `stale_since` tiap sumber, panjang queue tiap consumer, rate 429/503 dari Client-Facing API.

---

## Referensi

Dokumen ini disusun dari:
- `doc/IF4031 - Tugas Besar - M1.md` — spesifikasi resmi milestone 1
- `doc/IF4031_M1_Ingress.md` — laporan kerja kelompok (in progress, per 2026-09-30)
