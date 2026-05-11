# Tag 19 Lab -- Batch vs. Streaming: Architekturabwägungen

> :gb: [English version](README.md)

## Ziel

Baue sowohl einen Batch- als auch einen Streaming-Pfad über denselben Bestelldatensatz, vereine sie in einer Serving-Schicht und enkodiere die Architekturentscheidung als Empfehlungsfunktion. Kein Kafka -- "Streaming" liest hier Events aus einer Datei, damit du dich auf die Logik konzentrieren kannst.

**Bloom-Level: Analyze -> Evaluate.** Aufgaben 1-3 sind Apply-Niveau (du implementierst geteilte Primitive). Aufgaben 4-7 plus die Synthese-Aufgabe sind Analyze-/Evaluate-Niveau -- du argumentierst über Konsistenz, begründest eine Architektur und überprüfst die Invarianten der Serving-Schicht beim Reprocessing.

## Lernziele

Am Ende dieses Labs kannst du:

1. **Eine gemeinsame Cleaning-Funktion** implementieren, damit Batch- und Streaming-Pfad konsistent bleiben (Apply)
2. **Einen Batch-Pfad** implementieren, der eine CSV liest und nach Kategorie aggregiert (Apply)
3. **Einen Streaming-Simulationspfad** implementieren, der Events aus einer Datei verarbeitet und nach Kategorie aggregiert (Apply)
4. **Die Regel "Batch gewinnt bei Duplikaten"** der Serving-Schicht begründen, indem du nachvollziehst, was passiert, wenn Streaming und Batch in derselben Kategorie unterschiedliche Werte zeigen (Evaluate)
5. **Die Konsistenz** von Batch- und Streaming-Ergebnissen analysieren, indem du Kategorien als überlappend, batch-only oder stream-only klassifizierst (Analyze)
6. **Einen regelbasierten Architektur-Recommender kritisieren**, indem du Eingaben findest, bei denen die einzelne Ausgabe-Zeichenkette eine verletzte Anforderung verbirgt (Evaluate)
7. **Einen Kappa-Vorschlag kritisieren**, der mehrjähriges Reprocessing verlangt, und begründen, warum Lambdas Batch-Schicht die sicherere Wahl ist, wenn die Stream-Retention beschränkt ist (Evaluate)

## Was bereitgestellt wird

- `src/standalone/pipeline.py` -- Funktions-Stubs mit `raise NotImplementedError`
- `src/standalone/data/orders.csv` -- 20 Zeilen ShopFlow-Bestellungen
- `_get_sample_events()` und `run_pipeline()` -- Plumbing (bereits implementiert). `run_pipeline()` druckt jede Stufe, damit du siehst, was jede Funktion zurückgibt, während du sie ausfüllst.

> Dagster- und Airflow-Varianten dieser Pipeline werden an Tag 21 (Capstone) behandelt.

## Voraussetzungen

Reines Python-Lab -- **kein Spark, kein Java, kein Docker erforderlich**. `uv` erledigt den Rest.

## Setup und Ausführen

```bash
cd standalone
uv sync
uv run python -m standalone
```

Der erste Lauf scheitert mit `NotImplementedError` aus Aufgabe 1. Das ist die Schleife: Funktion implementieren, neu ausführen, nächste Stufe sehen, wiederholen.

---

## Aufgaben

### Aufgabe 1 -- `_clean_orders(rows)`

Implementiere das Cleaning, das sowohl Batch- als auch Streaming-Pfad nutzen:

- Zeilen mit fehlender `order_id`, `customer_id` oder `product_id` verwerfen
- Nur `quantity > 0` und `price > 0` behalten
- `revenue = quantity * price` hinzufügen
- `status` normalisieren (lowercase, trim)

### Aufgabe 2 -- `_aggregate_by_category(rows)`

Aus bereinigten Zeilen eine Liste von Dicts mit `category`, `total_revenue`, `order_count` erzeugen.

### Aufgabe 3 -- `batch_orders()`

`orders.csv` lesen, bereinigen, nach Kategorie aggregieren.

### Aufgabe 4 -- `streaming_orders()`

`_get_sample_events()` (bereits implementiert) simuliert den Stream. Cleaning und Aggregation sind identisch zum Batch-Pfad -- nur die Quelle unterscheidet sich.

### Aufgabe 5 -- `serving_layer(batch, streaming)`

Beide Ergebnisse mergen: konkatenieren, auf `category` deduplizieren (Batch ist Quelle der Wahrheit), nach `category` sortieren.

### Aufgabe 6 -- `consistency_check(batch, streaming)`

Gib ein Dict zurück mit:

- `overlapping_categories` -- Kategorien in beiden
- `batch_only` -- nur im Batch-Pfad
- `streaming_only` -- nur im Streaming-Pfad

### Aufgabe 7a -- `recommend_architecture(latency, budget, data_volume, reprocessing_needed)` (Apply, Aufwärmen)

Kodiere die Regel wortwörtlich, damit die Tests grün werden:

- Wenn `latency` `"hours"` ist oder `budget` `"low"` -> `"batch"`
- Sonst wenn `reprocessing_needed == False` und `data_volume != "large"` -> `"kappa"`
- Sonst -> `"lambda"`

Das ist nur das Aufwärmen. Die eigentliche Arbeit steckt in 7b, wo du die Regel hinterfragst.

### Aufgabe 7b -- Kritisiere den Recommender (Evaluate)

Eine einzelne Ausgabe-Zeichenkette kann nur ein Urteil tragen. Das verbirgt Fälle, in denen zwei Eingaben im Konflikt stehen und die Regel still einen Gewinner kürt. Deine Aufgabe: finde Eingaben, bei denen die Antwort der Funktion eine verletzte Anforderung verschluckt -- und schreibe auf, was du tatsächlich empfehlen würdest und warum.

Zwei Grenzfall-Szenarien. Führe für jedes die Funktion aus und halte deine Antwort in `SCENARIOS.md` (im Lab-Root anlegen) oder in einem Docstring-Block am Ende von `pipeline.py` fest:

1. `("seconds", "low", "small", False)` -> die Funktion sagt `"batch"`. Welche Eingabe hat die Regel ignoriert? Was impliziert die Anforderung `"seconds"`, das die Ausgabe nie widerspiegelt? Was würdest du tatsächlich empfehlen, und worauf zwingt das den Anwender umzuschwenken?

2. `("seconds", "high", "large", False)` -> die Funktion sagt `"lambda"`. Kappa wird ausschließlich wegen `data_volume == "large"` ausgeschlossen, aber dieses eine Signal kollabiert zwei Fragen: (a) kann dein Stream genug Historie vorhalten? (b) ist Lambdas operativer Overhead die Kosten wert? Wähle eine Annahme zur Stream-Retention, die deine Empfehlung auf Kappa kippt -- und eine, die sie bei Lambda hält.

Halte pro Szenario in ~3-5 Sätzen fest:

- **Welche Eingabe ignoriert die Regel?** Welcher der vier Parameter wurde still fallengelassen?
- **Welchen Tradeoff verbirgt die binäre Ausgabe?** Welche Kosten oder welches Risiko verschleiert die einzelne Zeichenkette?
- **Deine Empfehlung:** Was sagst du dem Team, und welche Annahme zwingst du sie damit, explizit zu machen?

Akzeptanz: `SCENARIOS.md` (oder der eingebettete Critique-Block) deckt beide Szenarien ab, und jede Antwort verweist auf mindestens ein Konzept jenseits der vier Eingabeparameter (Stream-Retention, Bedienpersonal, Recovery-Ziel, Wartungskosten zweier Codepfade, ...).

### Aufgabe 8 -- `write_results(results, name)` (und `_ensure_lake_dirs()`)

Schreibe die Aggregationsergebnisse auf die Platte, damit eine nachgelagerte Serving-Schicht sie lesen kann.

- `_ensure_lake_dirs()` legt `LAKE_DIR/batch/` und `LAKE_DIR/streaming/` an und gibt beide Pfade zurück.
- `write_results(results, name)` schreibt `results` als JSON nach `LAKE_DIR/<name>.json` und gibt den Pfad zurück.

So materialisiert eine echte Lambda-Pipeline jede Schicht in ihrem eigenen Verzeichnis, bevor die Serving-Schicht sie wieder einliest.

---

## Erfolgskriterien

- [ ] `uv run python -m standalone` läuft durch und druckt Batch-, Streaming-, Serving-Layer-, Konsistenzprüfungs-, Empfehlungs- und Write-Results-Output
- [ ] Du kannst erklären, warum `_clean_orders` gemeinsam genutzt wird
- [ ] Du kannst einen Workload nennen, bei dem Kappa die falsche Wahl ist, und warum
- [ ] Du kannst Lambda rechtfertigen, wenn `reprocessing_needed = True` und der Datensatz groß ist
- [ ] `SCENARIOS.md` (oder der eingebettete Critique-Block) deckt beide Grenzfälle aus Aufgabe 7b ab

---

## Synthese-Aufgabe -- Korrigierten Batch-Override nachvollziehen (Evaluate)

Diese Aufgabe rekombiniert zwei Primitive -- `serving_layer` (Aufgabe 5) und `consistency_check` (Aufgabe 6) -- in einem Szenario, das die Vorlesung nicht durchspielt: **ein korrigierter Batch-Lauf muss stream-only Output überschreiben, ohne stream-only Kategorien zu löschen, die der Batch nicht gesehen hat**. Kein neuer Code -- du benutzt die bereits implementierten Funktionen und hältst deine Argumentation schriftlich fest.

Szenario: Streaming liefert in einem fehlerhaften Fenster Aggregate für die Kategorien `{Electronics, Sports, Outdoor}`. Ein korrigierter Batch-Lauf produziert Aggregate nur für `{Electronics, Sports}` -- die Batch-Quelle hat für dieses Fenster keine `Outdoor`-Daten.

Aufgabe (~10 Minuten):

1. Schreibe in einem Python-REPL (oder in `SCENARIOS.md`) zwei Listen-Literale:
   - `corrected_batch` -- mit `Electronics` und `Sports`, aber mit **anderen** Revenue-/Order-Count-Zahlen als Streaming.
   - `streaming` -- mit allen drei Kategorien, inklusive `Outdoor`.
2. Rufe `serving_layer(corrected_batch, streaming)` auf und prüfe das Ergebnis. Bestätige, dass `Electronics` und `Sports` die Batch-Zahlen tragen und `Outdoor` mit seinen Streaming-Zahlen überlebt.
3. Rufe `consistency_check(corrected_batch, streaming)` auf und bestätige, dass `Outdoor` als `stream_only` auftaucht.
4. Schreibe in 3-5 Sätzen in `SCENARIOS.md`: **Warum funktioniert das für Lambda, und warum würde Kappa mit beschränkter Stream-Retention an genau diesem Szenario scheitern?**

Akzeptanz: `SCENARIOS.md` enthält die Antwort zu Schritt 4 und eine kurze Notiz (oder ein kopiertes REPL-Transkript), das das Override-Verhalten aus Schritten 2-3 zeigt.

---

## Stretch Goals (optional, kein neuer Code nötig)

1. **Ein echtes Szenario bewerten** -- wähle ein dir bekanntes Unternehmen (z. B. eine Liefer-App, ein internes Analytics-Dashboard bei der Arbeit). Gehe die Eingaben von `recommend_architecture` für dieses Unternehmen durch und schreibe 3-4 Sätze in `SCENARIOS.md`, ob du das Urteil der Funktion in einem Design Review wirklich verteidigen würdest.
2. **Einen dritten Grenzfall finden** -- konstruiere ein weiteres Eingabe-Tupel (über die zwei aus Aufgabe 7b hinaus), bei dem das Urteil der Funktion einen echten Tradeoff verschleiert. Trage es in `SCENARIOS.md` mit derselben Drei-Punkt-Struktur ein.
