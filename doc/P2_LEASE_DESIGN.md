# P2: lease, fencing, dan checkpoint polling

Tanggal: 2026-11-17. Owner: Brian. Isu: #282. Status: draft untuk review anggota lain. Pilihan P2: lease PostgreSQL dengan waktu server dan fencing transaksi. Dasar P1 sudah disepakati di `P1_STORAGE_DESIGN.md` (#289, PR #341): Postgres per node, 16 shard, RF=2, write ack dari minimal satu owner. Mekanisme fencing lintas shard harus disepakati dengan owner P1 sebelum #284 dinyatakan selesai.

## 1. Scope dan jaminan

Semua replika Aggregator melayani baca. Hanya holder lease sumber boleh polling dan menerima hasil ke storage. Satu lease per sumber (BMKG/PVMBG), identitas holder UUID baru per proses, bukan nama pod yang bisa dipakai ulang. BMKG memiliki checkpoint seismic dan warning terpisah dalam satu siklus.

Jaminan yang dituju: satu epoch menerima hasil baru pada satu waktu, hasil epoch lama ditolak, checkpoint siklus committed hanya maju sekali, dan takeover melanjutkan checkpoint durable. Batch belum committed boleh dibaca ulang dan ditulis idempoten.

**Exactly-once HTTP request pada crash arbitrer tidak dapat dijamin dengan kontrak upstream sekarang.** Jika upstream menerima GET lalu pemanggil mati sebelum mencatat hasil, penerus tidak tahu apakah request sampai. Retry dapat mengulangi request; tidak retry dapat kehilangan hasil. Lease mencegah polling rutin berlipat karena jumlah replika dan fencing melindungi hasil simpan, tetapi tidak membatalkan request yang sudah sampai upstream. Kontrak mock belum menyediakan idempotency key atau claim periode. Bukti upstream harus membedakan polling rutin dengan retry failover. Konfirmasi interpretasi kriteria 2 ke asisten; dedup DB bukan bukti tepat satu request dalam seluruh kondisi crash.

## 2. Alternatif dan keputusan

| Opsi | Waktu dan serialisasi | Fencing/checkpoint | Trade-off |
|---|---|---|---|
| Kubernetes Lease | renewTime ditulis client, update objek memakai kontrol concurrency API | Fencing storage dan checkpoint masih terpisah | Infrastruktur sudah ada, tetapi lease bukan transaksi ingest |
| Baris lease PostgreSQL, dipilih | Waktu server DB dan row lock | Epoch integer, validasi lease dan ingest bisa satu transaksi pada satu DB | Memakai teknologi M1; authority menjadi titik kegagalan; multi-shard tidak otomatis atomik |
| etcd terpisah | Transaksi linearizable, TTL server, revision sebagai epoch | Storage tetap harus memvalidasi epoch; hasil DB tidak atomik dengan transaksi etcd | Tambahan quorum, RAM dan operasi cluster; tidak memakai datastore internal k3s langsung |

client-go tidak menjamin fencing. Implementasinya mengamati perubahan record menggunakan waktu lokal dan toleran terhadap clock offset, dengan batas terhadap perbedaan laju jam. Jadi alasannya bukan menganggap renewTime dipercaya mentah oleh semua client. [client-go](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection), [Lease API](https://kubernetes.io/docs/reference/kubernetes-api/coordination/lease-v1/).

etcd memberi revision berurutan dan transaksi linearizable; penerima tulisan tetap perlu validasi token. [Jaminan API etcd](https://etcd.io/docs/v3.6/learning/api_guarantees/).

Keputusan ini memilih baseline P2 di Postgres yang sudah tersedia. Baseline satu DB dipakai untuk membangun dan menguji protokol lokal, lalu diintegrasikan dengan desain P1 yang sudah dipilih. Ini bukan izin menaruh seluruh katalog M2 di satu database atau bukti HA coordinator.

## 3. Waktu, clock skew, dan authority

DB authority menentukan expiry, jadwal periode dan penerimaan hasil. Pod tidak mengirim expiry hasil time.Now(). Timer Go hanya membangunkan percobaan renew/poll; setelah bangun/koneksi pulih, pod harus meminta otorisasi DB kembali.

Gunakan `clock_timestamp()` **setelah row lock diperoleh**, ambil sekali sebagai db_now untuk operasi itu. `now()` adalah waktu awal transaksi dan dapat usang setelah menunggu lock. Ini penyempurnaan usulan now() di outline awal. [Fungsi waktu PostgreSQL 16](https://www.postgresql.org/docs/16/functions-datetime.html).

Jam pod +45 s lalu -30 s tidak memengaruhi perbandingan authority. Jika node DB sendiri digeser, jam maju dapat mempercepat expiry dan jam mundur menunda takeover. Row lock/epoch tetap menserialisasi penerimaan, tetapi batas takeover 60 s tidak dijamin jika jam authority mundur tanpa batas. Sumber waktu tunggal bukan jam yang kebal gangguan. Dampak clock-skew pada DB harus diuji, bukan disembunyikan dengan pengecualian Tester Agent.

Authority tidak terjangkau berarti polling dan penerimaan hasil berhenti; read dari store yang tersedia tetap boleh. Tidak ada fallback lease lokal atau authority kedua independen. Epoch harus bertahan melewati restart. Restore backup lama memerlukan penghentian writer dan prosedur menaikkan epoch di atas semua token yang pernah diterbitkan. Kehilangan metadata authority belum ditangani oleh baseline ini.

## 4. Konfigurasi dan budget failover

| Nama | Default | Fungsi |
|---|---|---|
| `LEASE_TTL` | `15s` | Masa berlaku dari acquire/renew sukses di DB |
| `LEASE_RENEW_INTERVAL` | `3s` | Percobaan renew leader/acquire follower |
| `LEASE_DB_TIMEOUT` | `2s` | Batas operasi lease/lock DB |
| `LEASE_RENEW_DEADLINE` | `6s` | Tanpa konfirmasi renew, hentikan kerja lokal |
| `POLL_TIMEOUT_MS` | `4000` | Timeout per endpoint M1 |
| `POLL_INTERVAL_MS` | `3000` | Interval minimum siklus M1 |

Validasi semua positif, DB timeout < renew interval < renew deadline < TTL. Renew independen dari fetch/ingest. Jangan menahan row lock selama HTTP.

Dengan authority sehat dan jam stabil: expiry <=15 s dari renew terakhir, follower mencoba dalam 3 s, acquire <=2 s, sehingga holder baru sekitar <=20 s. Fetch BMKG dua endpoint memiliki budget 8 s lalu commit 2 s: target hasil baru sekitar 30 s. Ini budget desain, belum hasil ukur. DB outage, jam DB mundur, starvation, backlog dan OAuth recovery dapat melampauinya. TTL pendek mempercepat takeover tetapi meningkatkan churn akibat jeda DB/scheduler.

## 5. State dan protokol storage

State logis berikut belum merupakan migration yang dijalankan:

```text
source_lease(source PK, holder UUID nullable, epoch BIGINT,
             expires_at timestamptz, next_poll_at timestamptz)
poll_checkpoint(source, endpoint, cursor, version, PRIMARY KEY(source, endpoint))
poll_cycle(source, cycle_id, epoch, checkpoint_version, status,
           UNIQUE(source, cycle_id))
inbox(batch_id UNIQUE, payload, processing_status)
outbox(event_id UNIQUE, payload, publish_status)
```

Epoch awal 0; acquire pertama 1. Baris tidak dihapus saat release. Overflow BIGINT adalah hard failure, tidak wrap. DB menerbitkan cycle_id; retry batch yang sama mempertahankan ID itu. Retensi/penempatan inbox dan outbox mengikuti integrasi P1/P3.

### Acquire, renew, release

Acquire membuat baris awal memakai INSERT ON CONFLICT DO NOTHING. Dalam transaksi READ COMMITTED, SELECT FOR UPDATE baris sumber, lalu baca db_now. Jika holder masih valid, tolak acquire, termasuk acquire ulang holder sama. Jika kosong/expired, increment epoch, set holder dan expiry = db_now + TTL. Commit sebelum bekerja, kembalikan epoch dan checkpoint committed. Respons hilang dianggap hasil tidak pasti: baca state dan lakukan renew terverifikasi sebelum bertindak.

Renew mengunci baris sama, lalu memeriksa holder dan epoch **sama persis**, serta expires_at > db_now. Jika cocok, perpanjang dari db_now. Lease expired tidak dapat dihidupkan lagi dengan renew walau belum ada pengganti: harus acquire epoch baru. Kegagalan/ketidakpastian membuat client membatalkan fetch dan menonaktifkan batch generasi lokal itu sampai verifikasi selesai.

Release bersyarat holder/epoch, mengosongkan holder dan expiry, mempertahankan counter. SIGTERM menghentikan awal polling, membatalkan fetch, menyelesaikan transaksi pendek yang valid, lalu release. TTL menangani kematian mendadak.

### Siklus dan penerimaan hasil

Awal siklus memvalidasi authority, memeriksa next_poll_at, dan membuat reservasi cycle_id agar dua goroutine lokal tidak memulai fetch ganda. Maksimal satu siklus aktif per sumber. Setelah takeover, reservasi lama ditandai abandoned dan penerus mulai dari checkpoint committed. Reservasi periode tidak menyelesaikan ketidakpastian HTTP pada bagian 1.

Pada baseline satu DB, penerimaan hasil wajib satu transaksi storage:

1. Lock lease dan checkpoint dalam urutan tetap. Periksa waktu DB, holder, epoch, cycle_id aktif dan checkpoint_version dari awal fetch.
2. Tolak ketidakcocokan sebelum perubahan data. Token lebih kecil ditolak, token lebih besar dari authority juga ditolak, token sama hanya sah dengan holder benar dan lease belum expired. Tidak memakai perbandingan waktu pod.
3. Simpan payload mentah/parking yang diperlukan, upsert idempoten, buat outbox dengan ID kanonik **hasil DB**, tandai siklus committed, majukan cursor/versi, dan jadwalkan next_poll_at dari DB. Commit bersama.
4. Retry cycle_id committed mengembalikan receipt tanpa efek kedua setelah validasi izin. Timeout commit diselesaikan dengan membaca receipt, bukan menebak keberhasilan.

Acquire/renew/accept memakai lock sama, ditahan sampai transaksi berakhir. Acquisition berikutnya tidak dapat menyela transaksi hasil yang sudah tervalidasi; transaksi itu dilinearisasikan sebelum acquisition baru. Proses lama bisa masih merasa leader, tetapi efek tulisan usang ditolak. [Row locking PostgreSQL](https://www.postgresql.org/docs/16/explicit-locking.html).

Atur server-side statement_timeout, lock_timeout dan idle_in_transaction_session_timeout selain context Go, agar freeze tidak menahan lock selamanya. Pecah backlog menjadi batch kecil dan ukur durasi transaksi. Saat lease hilang, buang seluruh batch generasi lama; acquire baru harus membaca checkpoint baru sebelum fetch.

## 6. Freeze dan failover

```text
A acquire epoch 41, expires pada waktu DB 15
A mulai HTTP lalu VM freeze
waktu DB 15: lease A expired
B acquire epoch 42 dan lanjut dari checkpoint committed
A bangun dengan hasil HTTP epoch 41
storage lock lease, melihat epoch 42, menolak sebelum upsert/outbox/cursor
A membuang batch; acquire dan checkpoint baru diperlukan untuk kerja baru
```

Jika B belum mengambil alih, expiry tetap menolak hasil A. Jika transaksi A sudah memegang row lock sebelum freeze, B menunggu commit/rollback/timeout DB; epoch baru tidak diterbitkan sambil transaksi lama belum selesai. Node DB ikut freeze menghentikan liveness sampai authority pulih.

## 7. Cursor, publikasi, dan OAuth

Cursor maju hanya setelah data atau payload gagal utuh tersimpan durable. Enqueue channel memori bukan checkpoint. Crash sebelum commit mempertahankan cursor; respons commit hilang diperiksa lewat receipt. Unik `(source, source_ref_id)` mempertahankan ID kanonik. Outbox dibuat hanya untuk perubahan yang diterima; publisher confirm dengan event_id stabil dan consumer idempoten menangani redelivery.

PVMBG memakai reported_at > since. Gunakan overlap awal `POLL_CURSOR_OVERLAP=1s` dan dedup agar timestamp sama pada batas poll tidak hilang, simpan resolusi timestamp penuh. BMKG memerlukan cursor seismic dan warning sendiri; warning-only batch tidak boleh diabaikan. Overlap terbatas mengasumsikan keterlambatan timestamp terbatas. Timestamp retroaktif arbitrer perlu replay penuh/rekonsiliasi atau cursor upstream lebih kuat; no-gap tidak berlaku di luar kontrak itu.

Token manager PVMBG hanya berjalan di holder PVMBG setelah acquire. Token pair tersedia durable untuk penerus dengan akses terbatas ke Aggregator. Penulisan pair memvalidasi holder/epoch juga; respons refresh dari holder lama tidak boleh menimpa pair baru. Token tidak masuk log, isu, atau dokumen. Respons refresh hilang setelah token dikonsumsi mempunyai ketidakpastian eksternal yang tidak atomik dengan DB. Jangan mengandalkan bootstrap ulang memakai one-time code yang sudah terpakai. #283 harus memverifikasi recovery mock dan mencatat batasnya sebelum failover PVMBG dianggap selesai.

## 8. Integrasi P1 yang wajib diselesaikan

P1 memakai dual write dengan W1 (ack jika satu owner sudah commit) dan UUID v5 deterministik, bukan transaksi global antar semua store. Protokol transaksi bagian 5 konkret untuk satu DB. **Cek lease di DB A lalu tulis DB B tanpa koordinasi bukan fencing atomik.** `token >= max_seen` pada shard yang belum melihat epoch baru juga masih menerima leader lama. Memindahkan lease ke satu shard saja tidak cukup. ID deterministik mengatasi duplikat identitas, bukan otorisasi writer.

Kontrak integrasi #284 dengan owner #289:

- Semua ingest melalui penerimaan terfence, tidak ada direct write poller yang melewati validasi.
- Penerimaan batch, checkpoint dan receipt menjadi satu keputusan durable. Efek ke shard lain dapat berupa materialisasi idempoten dari batch yang sudah diterima, dengan ID/versi tetap dan epoch barrier sebelum writer baru aktif.
- Shard terputus tidak menerima tulisan lama atau kembali ke routing sebelum mengejar barrier/versi baru. Authority fail closed jika kepemilikan tidak dapat dipastikan.
- Replikasi/penempatan authority, inbox dan outbox tidak membuat satu node menyimpan seluruh katalog, atau kehilangan accepted batch saat wipe. Coordinator bukan salinan penuh permanen untuk menghindari P1.
- Ack replikasi, read partial dan rebalancing tetap keputusan P1. Jika engine P1 tidak mendukung kontrak ini, revisi desain sebelum implementasi #284. Transaksi lokal tidak membuktikan transaksi lintas DB.

#283 dapat mulai dari Compose. Klaim gabungan P1/P2 dan HA authority menunggu mekanisme konkret dan pengujian integrasi. Spike ini tidak menetapkan desain lintas area sepihak.

## 9. Verifikasi dan review

| Skenario | Bukti yang diperlukan |
|---|---|
| Tiga replika dan acquire bersamaan | Satu holder/epoch per sumber, hanya satu acquire sukses, log upstream tidak naik tiga kali |
| Kill leader | Ukur kill hingga penerus fetch/commit <=60 s, cek checkpoint dan ID |
| Pod clock +45 s lalu -30 s | Tidak ada dua epoch menerima hasil satu siklus, fencing rejection tercatat |
| Clock/freeze authority DB | Ukur batas liveness dan recovery metadata, jangan mengasumsikan jam DB stabil |
| Freeze holder >TTL, dengan/tanpa takeover | Hasil lama ditolak sebelum efek data/outbox/cursor |
| Freeze saat transaksi aktif | Timeout DB mengakhiri transaksi, successor tidak menembus lock |
| Crash sebelum/sesudah commit dan respons hilang | Receipt/checkpoint tidak maju dua kali, tidak ada event logis hilang/ganda |
| Timestamp batas, warning-only | Overlap/dedup dan cursor terpisah tetap menangkap data |
| Broker mati lalu hidup | Outbox menahan backlog dan drain tanpa kehilangan alert |
| Takeover saat OAuth refresh | Token penerus dan recovery invalid_grant dibuktikan |
| Partition/rejoin shard | Barrier selesai sebelum routing, stale writer ditolak pada setiap shard |

Log: source, holder, epoch, cycle_id, checkpoint_version, result/reason, correlation_id. Metrik: leader changes, renew errors, stale writes rejected, checkpoint lag, failover duration, outbox backlog. Tidak log credential/OAuth token.

Pemetaan: #283 election/poll/cursor/token lifecycle; #284 fencing/receipt/outbox bersama P1; #285 routing/readiness; #286 shutdown; #287 kill/skew/freeze; #288 load. Dokumen ini belum mengimplementasikan runtime atau membuktikan hasil chaos test.

- [x] Alternatif, keputusan, waktu, fencing, freeze, TTL/renew dan cursor ditulis.
- [ ] Review anggota selain penulis: nama, tanggal dan hasil belum tersedia.
- [ ] Konfirmasi interpretasi exactly-once HTTP dengan asisten.
- [ ] Sepakati mekanisme integrasi authority dan batch dengan owner P1, terutama kombinasi W1, epoch barrier dan repair loop.

#282 tetap terbuka sampai review yang diwajibkan dilakukan. Batas integrasi dipindahkan ke pekerjaan implementasi atau diselesaikan melalui revisi desain, bukan dihapus dari laporan.
