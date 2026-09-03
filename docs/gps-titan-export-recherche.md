# Recherche: Rohdaten-Export aus Titan Sports GPS / Hudl

Stand: 2026-09-03. Ziel: Weg finden, um Rohpositionen (idealerweise 10 Hz, UTC-Zeitstempel) pro
Spielerin für GER vs. Panama Rojo und GER vs. Puerto Rico (beide 16.05.2026) aus dem Titan-Portal
zu bekommen — für (a) Validierung der Drohnen-Tracking-Positionen und (b) Identitäts-Ground-Truth
für die ReID-Challenge (`docs/hackathon-challenge-reid.md`).

**Kurzfazit vorab:** Es gibt in der öffentlich zugänglichen Titan/Hudl-Dokumentation **keinen
Hinweis auf einen Rohdaten-Export (CSV/API mit Einzelpositionen)**. Das deckt sich mit der
Beobachtung des Users im Portal. Der plausibelste Weg ist eine direkte Support-Anfrage an Hudl,
nicht ein verstecktes Menü.

---

## 1. Produkt-Identifikation

„Titan Sports" (auch „Titan Sensor", Marke TITAN, Website vormals `titansensor.com` /
`titansports.io`) war ein eigenständiger Hersteller von GPS-Trackerpods für Teamsport
(Fußball, American Football, Basketball, Volleyball). **Hudl hat Titan Sports am 3. Juni 2025
vollständig übernommen** [VERIFIED: businesswire.com, siehe Suchergebnis-Snippet]. Seitdem läuft
die Integration in zwei Schritten:

- Video-Integration (GPS-Overlay auf Hudl-Videotimeline) ist für Fußball ("soccer") live, für
  American Football laut Ankündigung "coming soon" [CITED: hudl.com/blog/titan-in-hudl-product-reveal].
- Die alte Titan-Domain `titansports.io/app.html` leitet inzwischen (301) direkt auf
  `hudl.com/products/titan` weiter [VERIFIED: WebFetch-Redirect beobachtet, 2026-09-03] — die
  eigenständige Titan-Marketingseite existiert nicht mehr getrennt von Hudl.

**Wichtig für den konkreten Fall:** Die Spiele wurden am 16.05.2026 getrackt, also **fast ein Jahr
nach der Übernahme**. Es ist unklar, ob der Verband zu diesem Zeitpunkt noch das alte,
eigenständige Titan-Portal (`titansensor.com`, teils noch aktiv laut Suchtreffern) nutzte oder
bereits die neue, in Hudl integrierte Oberfläche [ASSUMED — nicht verifizierbar ohne Blick in den
tatsächlichen Account]. Das ist relevant, weil beide Portale unterschiedliche Export-Optionen
haben könnten und die Support-Anfrage entsprechend adressiert werden sollte (siehe Abschnitt 3).

Was das Portal laut Marketingmaterial bietet (Team Summary Dashboard, Player Reports, "3D and
satellite field views", Comparison Charts) [CITED: titansports.io/app.html-Inhalt, per Suchindex
zitiert, da die URL selbst inzwischen weiterleitet] passt exakt zur Beschreibung des Users: eine
"field view" mit beweglichen Spielerpunkten. Das ist vermutlich genau die Ansicht, die verwendet
wurde — sie ist zur Visualisierung gebaut, nicht zum Datenexport.

**Export/API — was öffentlich dokumentiert ist:**

| Quelle | Aussage zu Export/API |
|---|---|
| Titan FAQ (hudl.com/products/titan/faq) | Keine Erwähnung von Export, CSV, API, Sampling-Rate oder Rollen/Rechten [CITED, negative Aussage — s. Vorbehalt unten]. |
| "Titan is Now Inside Hudl" (Produkt-Ankündigung) | Nennt "Session reports, leaderboards, and individual athlete metrics", GPS-Overlay auf Videotimeline. Kein Wort zu Rohdaten-Export oder API. [CITED: hudl.com/blog/titan-in-hudl-product-reveal] |
| "Titan GPS + Hudl" Blogpost | Ein Nebensatz: "customizable dashboards and **seamless exports**" — vage, vermutlich Report-/Summary-Export, nicht zwingend Rohpositionen. [CITED: hudl.com/blog/hudl-titan — Formulierung ist Marketingsprache, keine Funktionsbeschreibung] |
| "Best GPS Tracker for Football" Blogpost | Nennt Heatmaps, 3D-Replays, Leaderboards — keine Export-/API-Erwähnung. [CITED] |

**Vorbehalt zu den negativen Aussagen:** Alle vier Quellen sind Marketing-/FAQ-Seiten, keine
technische API-Dokumentation. "Nicht erwähnt" ist kein Beweis für "existiert nicht" — es ist
plausibel, dass ein Rohdaten-Export existiert, aber nur für zahlende Enterprise-Kunden oder auf
Anfrage freigeschaltet wird und deshalb nicht öffentlich beworben wird (so handhaben es andere
Anbieter, siehe Abschnitt 2). Das spricht für den direkten Support-Weg statt für weiteres Suchen
im Marketing-Material.

Eine dedizierte Titan-API bzw. Entwicklerdokumentation war über Suche nicht auffindbar. Treffer zu
"Titan API" betrafen andere, gleichnamige Produkte (Titan DMS, ServiceTitan, Titan GPS Fleet von
Certified Tracking Solutions — ein Flottenmanagement-Anbieter, **nicht** derselbe Titan wie der
Sport-GPS-Hersteller). Diese Verwechslungsgefahr ist real: mehrere Firmen heißen "Titan GPS" bzw.
"Titan [Sports/Sensor]"; bei jeder weiteren Recherche/Support-Anfrage explizit "Titan Sports /
TITAN athlete tracker, jetzt Teil von Hudl" spezifizieren.

## 2. Übliche Export-Granularität bei Teamsport-GPS (Vergleich)

Zum Vergleich, was in der Branche als Standard gilt — das zeigt, wonach beim Titan-Support
konkret gefragt werden sollte:

- **Catapult (OpenField):** Explizit dokumentierter Rohdaten-Export. Im OpenField Console:
  Rechtsklick auf Athletin → "Export Sensor CSV" → 10-Hz-GPS- und Inertialdaten als CSV.
  Zusätzlich "Exporting High Frequency (100Hz) Data" für IMU-Kanäle, sowie eine Cloud-API
  (OpenField Cloud API, R-Paket `catapultR`) und "Catapult Connect" für Systemintegration.
  [CITED: support.catapultsports.com/hc/en-us/articles/360001427755,
  support.catapultsports.com/hc/en-us/articles/360001637196, catapultr.catapultsports.com]
- **STATSports / KINEXON:** Laut Marktvergleich ebenfalls 10-Hz-GNSS-Erfassung (STATSports) bzw.
  UWB/LPS + GPS (KINEXON) mit Athletenmanagement-Export; Details nicht im Detail verifiziert,
  MEDIUM-Konfidenz aus Vergleichsartikel. [CITED: gamecode.ai/insights/articles/catapult-vs-statsports-vs-kinexon]
- **Titan:** Kein öffentlich dokumentiertes Äquivalent zu "Export Sensor CSV" oder einer Cloud-API
  gefunden. Das Produkt ist explizit auf Coach-Dashboards ausgerichtet ("coaches without dedicated
  sports science staff"), nicht auf Sportwissenschafts-Rohdatenanalyse — das erklärt plausibel,
  warum die Exportfunktion (falls vorhanden) nicht prominent beworben wird. [ASSUMED — Schluss aus
  Positionierung, nicht aus expliziter Aussage]

**Sampling-Rate der Titan-Hardware:** Das (ältere) Titan 1+ läuft mit 10 Hz GPS; das Titan 2+
wurde 2020 mit 25-Hz-GPS-Sampling beworben ("industry leading") [CITED: news.titansports.io/2020/07/28/79 —
Inhalt der Seite über Suchindex zitiert, Direktabruf der Domain schlug 2026-09-03 mit DNS-Fehler
fehl, d. h. die alte News-Subdomain scheint inzwischen abgeschaltet]. Welches Pod-Modell der
Verband 2026 im Einsatz hatte (Titan 1+, 2, 2+, oder ein neueres Modell) ist unbekannt — das
bestimmt, ob 10 Hz oder bis zu 25 Hz Rohdaten überhaupt existieren. Beim Coach erfragen.

**Koordinatensystem / Zeitbasis:** Nicht dokumentiert gefunden. Branchenüblich (Catapult, STATSports)
ist die interne Speicherung als WGS84 Lat/Lon plus Device-Zeitstempel (meist GPS-Zeit, die exakt
UTC-synchron ist, abzüglich Sprungsekunden-Offset — in der Praxis vernachlässigbar), mit
Umrechnung auf lokale Feldkoordinaten erst für die Visualisierung. Für Titan ist das plausibel,
aber **nicht verifiziert** — muss beim Support explizit erfragt werden, falls ein Export gelingt.
[ASSUMED]

## 3. Konkrete nächste Schritte, priorisiert

**Rang 1 — Direkte Hudl/Titan-Support-Anfrage, konkret formuliert.**
Das ist der Weg mit der höchsten Erfolgswahrscheinlichkeit, weil öffentliche Dokumentation keinen
Self-Service-Export zeigt, aber "seamless exports" als Formulierung existiert und Sportwissenschafts-
Use-Cases (Validierung gegen ein anderes Trackingsystem) ein Standard-Anwendungsfall bei GPS-Anbietern
sind. Kontaktweg: `support.hudl.com/s/contactsupport` [CITED: support.hudl.com/s/contactsupport?language=en_US].
Anfrage sollte explizit enthalten:
  - "Titan athlete GPS tracker (formerly Titan Sports, now part of Hudl)" — zur Abgrenzung von
    gleichnamigen Fremdprodukten.
  - Team-/Account-Name des Verbands, Datum und Gegner beider Sessions (16.05.2026, GER–Panama Rojo,
    GER–Puerto Rico), damit Support die Sessions direkt findet.
  - Explizite Frage nach: "raw per-athlete GPS position export at native sampling rate (10 Hz or
    higher), including timestamp, latitude/longitude (or local pitch coordinates) — as CSV or via
    API", nicht nach Summary-Metriken.
  - Frage, ob dafür eine bestimmte Rolle/Berechtigung (Org-Admin statt Team-Viewer) oder ein
    Zusatz-Tarif nötig ist.
  - Hinweis auf wissenschaftlichen/Validierungs-Zweck — erhöht laut Erfahrungswerten bei anderen
    Sport-Tech-Anbietern (Catapult, STATSports) die Chance auf eine Sonderfreigabe. [ASSUMED,
    basierend auf allgemeiner Branchenpraxis, nicht Titan-spezifisch verifiziert]

**Rang 2 — Über den Kopftrainer/Verbandsadmin die Account-Rolle prüfen bzw. hochstufen lassen.**
Falls der User nur "Team-"/Viewer-Zugriff hat: In vergleichbaren Plattformen (Catapult, Hudl selbst)
sind Export-/API-Funktionen oft an Org-Admin-Rechte gebunden, nicht an normale Team-Zugriffe
[ASSUMED, Analogieschluss]. Zu prüfen: Gibt es im Portal unter Team-/Org-Einstellungen (nicht in der
Spielansicht) einen separaten Bereich "Data" / "Integrations" / "API Keys"? Das ist der naheliegendste
Ort für eine evtl. vorhandene, aber versteckte Exportfunktion — im Session-/Field-View selbst wurde
laut User-Beobachtung nichts gefunden, was zur Beobachtung passt, dass Coach-Dashboards und
Admin-/Datenverwaltungsbereiche in solchen Produkten meist getrennt sind.

**Rang 3 — Prüfen, ob noch das alte, eigenständige Titan-Portal (titansensor.com) statt der neuen
Hudl-Oberfläche genutzt wird.** Da die Migration erst seit Mitte 2025 läuft, könnte der
Verbands-Account noch auf der Legacy-Instanz laufen, die andere (ggf. mehr) Exportoptionen als die
neue, vereinfachte Hudl-Coach-Oberfläche hat, oder umgekehrt weniger. Login-URL/Portal-Namen beim
Coach erfragen ("Wie loggst du dich ein — hudl.com oder eine andere Adresse?"), das grenzt ein, mit
welcher Hudl-Support-Kategorie ("Titan legacy" vs. "Get Started Titan" — beide Kategorien existieren
parallel im Hudl-Supportsystem) die Anfrage zu stellen ist.
[CITED: hudl.my.site.com/support/s/topic/... "titan-legacy" und "getstartedtitan" Topic-URLs
existieren beide, Inhalt der Artikel selbst war per WebFetch nicht abrufbar (JS-Rendering-Fehler auf
Toolseite), Existenz zweier getrennter Topics aber über Suchindex bestätigt.]

**Rang 4 — Fallback: Screen-Capture des Field-View und Reverse-Engineering.**
Nur falls Rang 1–3 scheitern. Bewertung: **nicht empfohlen als Primärweg.**
  - Technisch: Player-Punkte müssten per Video-Aufzeichnung des Bildschirms + eigener
    Homographie-Kalibrierung (das Feld im Field-View hat vermutlich feste Proportionen) in
    Feldkoordinaten zurückgerechnet werden. Zeitliche Auflösung wäre auf die Redraw-/Framerate der
    UI begrenzt (oft nur 1–2 Hz bei Web-Dashboards, nicht die native 10/25-Hz-Sensorrate) — für den
    Validierungs-Zweck (Vergleich mit CV-Tracking bei ~15 cm Median-Fehler) vermutlich zu grob.
  - Rechtlich/vertraglich: Vermutlich Verstoß gegen die Hudl-Nutzungsbedingungen (Scraping/
    automatisiertes Auslesen ist bei SaaS-Plattformen praktisch immer untersagt) — wurde nicht im
    Detail geprüft, aber als Risiko zu benennen. [ASSUMED — AGB nicht eingesehen]
  - Empfehlung: Nur als allerletzte Notlösung, und dann nur für die Identitäts-Ground-Truth-Frage
    (Frage 5b unten), nicht als Positions-Validierungsquelle.

## 4. Sync mit Drohnenvideo — Machbarkeit

Zwei getrennte Nutzungen der GPS-Daten, unterschiedliche Anforderungen an die Synchronisation:

**a) Identitäts-Ground-Truth (für ReID-Challenge):** Deutlich niedrigere Sync-Anforderung. Wenn pro
Spielzug die 5 deutschen Spielerinnen anhand grober Cluster-Bewegung (wer bewegt sich wann/wohin)
den GPS-Tracks zugeordnet werden können, reicht eine Genauigkeit im Sekundenbereich — die Frage ist
"welche Spielerin ist das", nicht "wo genau steht sie". Realistische Anker für die Zeitzuordnung:
  - Snap-Zeitpunkt: markanter, synchroner Bewegungsimpuls aller 10 Feldspielerinnen — sollte sowohl
    in der Drohnenspur als auch im GPS-Beschleunigungssignal klar erkennbar sein, guter Ankerpunkt.
  - Hudl-Clip-Erstellungsmetadaten: unklar, ob Hudl-Clips einen absoluten Erstellungszeitstempel
    intern führen (auch wenn er im UI nicht angezeigt wird) — beim Support mit erfragen, das wäre der
    sauberste Weg (direkte UTC-Ankerung statt Bewegungsmuster-Matching).
  - Ohne verlässlichen absoluten Zeitstempel: manuelles Matching pro Spielzug (Video-Encoder hat
    ~61 Clips für ein Spiel, siehe `docs/hackathon-challenge-reid.md`) ist aufwändig, aber bei nur
    zwei Spielen (16 bzw. weniger relevante Clips für 5-gegen-5-Ground-Truth) machbar.

**b) Positions-Validierung (GPS als Ground Truth für CV-Genauigkeit):** Hohe Anforderung — hier
zählt die Frage aus den Research-Questions, ob GPS überhaupt präzise genug ist. Konsumenten-/
Team-GPS liegt in der Literatur typischerweise bei **~1–3 m Positionsfehler** unter freiem Himmel,
teils schlechter bei schnellen Richtungswechseln (genau das Bewegungsmuster im Flag Football)
[CITED, allgemeine Literatur zu GPS-Validität im Teamsport: scienceforsport.com/gps-wearables-validity-and-reliability,
Titan-1+-spezifische Validierungsstudie existiert (journal.iusca.org, Titel: "The Accuracy of the
Titan 1+ 10 Hz Global Positioning System for Measures of..."), Volltext-Zahlen konnten technisch
nicht extrahiert werden (PDF-Parsing schlug fehl) — **als Aufgabe offen**, Paper sollte manuell
gelesen werden, bevor GPS als Positions-Ground-Truth verwendet wird].

Das eigene CV-System erreicht laut `docs/pilot-accuracy.md`/`docs/hackathon-challenge-reid.md`
einen Median-Fehler von ~0,17 Yards (~15 cm). Selbst im günstigsten Fall (1 m GPS-Fehler) ist GPS
damit **um ~das 6-Fache ungenauer als die eigene Drohnen-Pipeline** — GPS taugt bei dieser
Fehlerrelation nicht als Positions-Ground-Truth zur Feinvalidierung, sondern höchstens als grober
Plausibilitätscheck (liegt die CV-Spur in der richtigen Größenordnung, keine systematische
Verzerrung) und — mit Vorbehalt zur zeitlichen Zuordnung — als Identitätsanker. Diese Einschätzung
ist die wichtigste Antwort auf Forschungsfrage 4 und sollte die Erwartungshaltung an das
GPS-Datenprojekt insgesamt dämpfen: Der Hauptwert liegt in (b) Identität, nicht in (a) Positionsgenauigkeit.

## 5. Datenschutz (DSGVO)

GPS-Bewegungsdaten einzelner, identifizierbarer Spielerinnen sind personenbezogene Daten im Sinne
der DSGVO (Art. 4 Nr. 1) — Standort- und ggf. Gesundheits-/Leistungsdaten gelten als besonders
schutzwürdig, auch wenn sie nicht zwingend "besondere Kategorien" (Art. 9) sind [ASSUMED — allgemeine
DSGVO-Systematik, keine spezifische Rechtsprechung zu Sport-GPS-Daten recherchiert]. Für dieses
Projekt konkret relevant:

- Die bestehende Verbandsfreigabe vom 2026-08-31 (`docs/freigabe-vorlage.md`) wurde für **Video**-
  Nutzung zur Analyse eingeholt. Ob sie GPS-Bewegungsdaten (ein anderer, zusätzlicher Datentyp mit
  eigener Erhebungsquelle — Hudl/Titan als datenverarbeitende dritte Partei, nicht das eigene
  Drohnenmaterial) automatisch mit abdeckt, ist **fraglich** — das sollte nicht angenommen werden.
- Empfehlung: Die GPS-Nutzung für die Hackathon-Challenge (Identitäts-Ground-Truth, potenziell
  Weitergabe an Hackathon-Teilnehmende) **explizit in der schriftlichen Freigabe benennen**, nicht
  stillschweigend unter "Videomaterial" mitlaufen lassen. Das entwarf-Mail an den Head Coach sieht
  laut Aufgabenstellung bereits vor, das anzusprechen — diese Recherche bestätigt, dass das nötig
  ist, nicht optional.
- Zusätzlich zu klären (nicht Teil dieser Recherche, aber Folgefrage): Wer ist datenschutzrechtlich
  verantwortlich für die GPS-Rohdaten bei Hudl/Titan — der Verband als Kunde (Auftragsverarbeitung
  durch Hudl) oder Hudl selbst? Das bestimmt, ob für einen Datenexport zusätzlich zur
  Trainer-Freigabe eine Zustimmung des Verbands als Vertragspartner von Hudl nötig ist.

## Offene Punkte / Annahmen, die noch verifiziert werden müssen

| # | Annahme | Risiko falls falsch |
|---|---|---|
| A1 | Titan bietet keinen Self-Service-Rohdaten-Export im Portal | Falls doch vorhanden (nur versteckt), verschwendet Rang-1-Anfrage Zeit — aber Support-Anfrage würde das ohnehin aufdecken |
| A2 | Verbands-Account läuft auf 10 oder 25 Hz Pod-Hardware | Bestimmt maximal mögliche Zeitauflösung — beim Coach erfragen, welches Titan-Modell verwendet wurde |
| A3 | Koordinatensystem ist WGS84 Lat/Lon, konvertierbar auf Feldkoordinaten | Falls nur proprietäre lokale Koordinaten ohne Kalibrierungsreferenz exportiert werden, ist eigene Homographie zum Feld nötig |
| A4 | GPS-Fehler liegt bei ~1–3 m (nicht Titan-spezifisch verifiziert) | Titan-1+-Studie (journal.iusca.org) sollte für exakte Zahl gelesen werden, bevor GPS als Validierungsreferenz kommuniziert wird |
| A5 | Bestehende Verbandsfreigabe deckt GPS-Daten nicht automatisch ab | Falls doch abgedeckt, ist der zusätzliche Freigabe-Schritt unnötiger Mehraufwand — aber sicherer Default ist, explizit nachzufragen |

## Quellen

- [Hudl Completes Acquisition of Titan Sports — Businesswire](https://www.businesswire.com/news/home/20250603230805/en/Hudl-Completes-Acquisition-of-Titan-Sports-Expanding-Performance-Tracking-Ecosystem)
- [Complete the Picture: Titan is Now Inside Hudl](https://www.hudl.com/blog/titan-in-hudl-product-reveal)
- [Titan GPS + Hudl: Bringing Physical Performance Data to Teams at Every Level](https://www.hudl.com/blog/hudl-titan)
- [Titan • Frequently Asked Questions](https://www.hudl.com/products/titan/faq)
- [Titan GPS Athlete Trackers for all levels • Hudl](https://www.hudl.com/products/titan)
- [The Best GPS Tracker for Football: A Guide for Coaches](https://www.hudl.com/blog/best-gps-tracker-football)
- [TITAN 2+ delivers industry leading 25 Hz GPS sampling rate](https://news.titansports.io/2020/07/28/79/) (Domain 2026-09-03 nicht direkt erreichbar, Inhalt über Suchindex zitiert)
- [Contact Hudl Customer Support](https://support.hudl.com/s/contactsupport?language=en_US)
- [Hudl Support — Titan Legacy Topic](https://support.hudl.com/s/topic/0TOVY000000BTQ54AO/titan-legacy?language=en_US)
- [Hudl Support — Get Started Titan Topic](https://hudl.my.site.com/support/s/topic/0TOVY000000BYET4A4/getstartedtitan?language=en_US)
- [Exporting 10Hz Sensor Data from the OpenField Console — Catapult Support](https://support.catapultsports.com/hc/en-us/articles/360001427755-Exporting-10Hz-Sensor-Data-from-the-OpenField-Console)
- [Exporting High Frequency (100Hz) Data — Catapult Support](https://support.catapultsports.com/hc/en-us/articles/360001637196-Exporting-High-Frequency-100Hz-Data-from-the-OpenField-Console)
- [Catapult vs. STATSports vs. KINEXON](https://gamecode.ai/insights/articles/catapult-vs-statsports-vs-kinexon/)
- [GPS (Wearables) - Technology, Validity and Reliability | Science for Sport](https://www.scienceforsport.com/gps-wearables-validity-and-reliability/)
- [The Accuracy of the Titan 1+ 10 Hz GPS — IUSCA Journal](https://journal.iusca.org/index.php/Journal/article/download/326/421/5020) (Volltext nicht ausgewertet, s. offene Punkte)
- Interne Referenz: `docs/hackathon-challenge-reid.md`, `docs/freigabe-vorlage.md`, `docs/pilot-accuracy.md`
