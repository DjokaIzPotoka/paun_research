# CLAUDE.md — zora-research

Pravila za rad u ovom repou. Pročitaj pre svake sesije.

## 1. Šta je ovaj repo

Python research engine za ZORA hipoteze. Trenutno aktivna: **faza 0 (edge spike) za H-A1a i H-A1c** na Deribit BTC opcijama.

Izvori istine, ovim redom:
1. `research/hypothesis/options_rv/prereg.yaml` — obavezujuća specifikacija faze 0. Svi pragovi, definicije i parametri testa dolaze odavde.
2. `docs/phase-0-edge-spike.md` — čitljiv opis istog; ako se razlikuje od YAML-a, važi YAML, a razliku prijavi.
3. Mandat i odluke (`D-xx`) su u Notionu i u repou `zora-options-rv`; ovde se samo citiraju.

## 2. Tvrde zabrane u fazi 0 (D-58)

`prereg.yaml` je zaključan (tag `prereg/phase-0`). **Ne menjaj ga.** Ako nešto u njemu ne može da se implementira, stani i prijavi.

Do taga `data/phase-0` (Lock B, 2026-10-22) **zabranjeno je** računati ili prikazivati:
- residual, epizode, markout, PnL, `X_d`, p-vrednosti,
- bilo koju kolonu izvedenu iz ishoda.

Dozvoljeno pre Lock B: učitavanje, checksum, brojanje redova, pokrivenost po danu, nedostajući podaci, provera pravila minuta i validnosti kvota, forward, SVI fit, σ_model, spread model.

Kod koji računa ishode mora proveriti da tag `data/phase-0` postoji i odbiti da radi ako ne postoji.

## 3. Kauzalnost

Svaki izračun u trenutku t koristi samo kvote sa `venue_ts <= t`. Važi za forward, SVI, σ_model, izbor kandidata, detekciju epizoda i hedž. Izuzetak je samo izlazna kvota za markout, po `exit_quote` pravilu iz YAML-a. Fit celog dana, pa primena unazad, je greška.

## 4. Struktura i granice (D-55)

```
research/
  data/        ingest, jedna šema
  state/       forward, iv, greeks, surface_fit — čisto, deterministično
  eval/        troškovi, markout, statistika
  report/      izveštaj runa
  hypothesis/  base.py + po jedan folder po hipotezi
scripts/       jednosmerno: importuje research/, nikad obrnuto
runs/          u gitu samo report.md i config.yaml po runu
docs/          samo faza 0
```

Granice (import-linter u CI):
- `hypothesis/*` importuje samo `state` i `hypothesis.base`
- `eval` ne importuje `hypothesis/*`
- `state` ne importuje `data`
- `scripts/` → `research/`, nikad obrnuto

## 5. Pravila čistoće

- Nema `utils/`, `common/`, `helpers/`. Funkcija živi u modulu koji ju je prvi upotrebio; seli se tek kad je zatraži treći poziv iz drugog modula.
- Makefile cilj postoji samo ako ga CI poziva.
- Bez podrazumevanih vrednosti u kodu i bez `.env` fallbacka (P-15). Nedostajući parametar obara start sa jasnom porukom.
- Jedan `config.yaml` po runu, hashovan u izveštaj.
- Cene i količine u `state` su male dataclass omotnice sa valutom; konverzija traži eksplicitan izvor kursa. Opcije su inverse (cene u BTC).
- Nove zavisnosti samo uz pitanje.

## 6. Podaci

- Collector (AWS VM): `/var/lib/zora-l1/`, satni parquet fajlovi; povlače se lokalno i učitavaju sa `scripts/load_l1.py`.
- Lokalni ClickHouse: docker compose u repou, baza `optrv`.
- Sirovi podaci se nikad ne menjaju niti brišu; normalizacija je kod i može se ponoviti.

## 7. Stani i prijavi (P-12)

Ne popravljaj tiho i ne pretpostavljaj. Stani kad:
- specifikacija i kod ili podaci ne mogu da se usklade,
- treba vrednost koja nije u YAML-u ili configu,
- korak bi zahtevao nešto iz liste zabrana u §2,
- podaci izgledaju drugačije nego što YAML pretpostavlja (kolone, jedinice, vreme).

Na kraju svake sesije: šta je urađeno, sve pretpostavke, sve na čemu si stao.

## 8. Git

- Grane: `feat/...`, `fix/...`; kratko žive.
- Komitovi: conventional commits (`feat(eval): ...`).
- Tagovi: `prereg/phase-0`, `data/phase-0`, `gate/g-0`. Nikad ne pomeraj niti briši postojeći tag.
- `main` zaštićen; bez direktnog push-a.
