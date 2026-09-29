# ArmoniK Broker — protocole v1

Contrat entre le serveur `Broker/` (Rust) et son client `Adaptors/Broker/` (C#).
Il n'y a pas de génération de code : ce document et les vecteurs de `Broker/conformance/` sont
la seule source de vérité.

## 1. Transport

- HTTP/1.1 et HTTP/2 (h2 via ALPN, ou h2c en clair pour le développement). Même API.
- TLS optionnel ; mTLS quand une autorité cliente est configurée. Aucune autorisation par opération.
- Corps en `application/json`, UTF-8. Tailles de corps plafonnées par `max_body_bytes` (64 Kio par défaut).
- Toute réponse porte l'en-tête **`X-Broker-Epoch`** : entier non signé 32 bits en décimal, tiré au
  hasard au démarrage. Son changement signifie que le contenu de la file a été perdu.

## 2. Version

Toutes les routes sont préfixées par `/v1`. Une seule version est servie à la fois. Une requête vers un
autre préfixe `/vN` reçoit `404` avec le type `unsupported-version`. Le broker est une instance unique
dont la file est perdue au redémarrage (§1) : il n'y a pas de mise à jour progressive à traverser, et le
client traite ce `404` comme toute erreur définitive.

## 3. Conventions de valeurs

| Valeur | Règle |
|---|---|
| Partition, clé de répartition, identifiant de noeud | chaîne UTF-8 non vide, au plus 100 octets |
| Identifiant de tâche | chaîne UTF-8 non vide, au plus 59 octets en place, jusqu'à 512 octets via débordement |
| Priorité | entier de 1 à 16, 16 = plus urgent. Hors plage : `409 invalid-priority` |
| Durées | millisecondes (`*_ms`), entiers non signés |
| Tailles | octets (`*_bytes`), entiers non signés 64 bits |
| Taille encodée | entier 0 à 255 (§8.3) ; 0 = absence |
| Hachage d'identifiant | entier non signé 32 bits (§8.2) |
| Jeton | chaîne opaque ; le client ne doit ni la construire ni l'interpréter |

Champs inconnus : ignorés. Champs obligatoires absents ou mal typés : `400 malformed`.

## 4. Erreurs

Corps `application/problem+json` (RFC 9457), à usage de diagnostic :

```json
{ "type": "urn:armonik:broker:backpressure", "title": "backpressure", "status": 429,
  "detail": "hard memory threshold reached" }
```

**Le client décide d'après le seul statut** : `429` et `503` se réessaient, tout autre statut d'erreur
est définitif. Le type et le détail ne servent qu'à journaliser.

| Type (`urn:armonik:broker:…`) | Statut | Signification | Conduite du client |
|---|---|---|---|
| `malformed` | 400 | corps invalide, jeton illisible, valeur hors format | bug client : journaliser, échouer |
| `unsupported-version` | 404 | préfixe de version non servi (§2) | échouer |
| `not-found` | 404 | route inconnue | bug client : échouer |
| `invalid-priority` | 409 | priorité hors de 1 à 16 | rejeter la soumission |
| `partition-limit` | 409 | nombre maximal de partitions atteint | échouer : c'est une limite de configuration |
| `payload-too-large` | 413 | corps au-delà de `max_body_bytes` | découper le lot (§6.1) |
| `backpressure` | 429 | seuil mémoire dur, réserve de clés pleine | attendre `Retry-After`, rejouer le lot entier |
| `overloaded` | 503 | anneau d'acteur plein, requêtes concurrentes en excès | attendre `Retry-After`, rejouer |
| `shutting-down` | 503 | arrêt en cours | attendre `Retry-After`, rejouer |

`429` et `503` portent `Retry-After` (secondes). Un statut d'erreur est définitif : **rien n'a été
appliqué**, un rejeu ne crée pas de doublon. Le lot d'enqueue est atomique.

La livraison est **au moins une fois** : une requête dont la réponse se perd (coupure, timeout) est
rejouée par le client alors qu'elle a pu être appliquée, et un enqueue rejoué peut alors enfiler une
seconde fois les mêmes tâches. Core tolère ces doublons, comme avec les autres files.

## 5. Conduite du client

Le protocole est sans état côté client : aucun enregistrement, aucun identifiant de session. Chaque
requête porte tout ce dont le serveur a besoin.

- **Réessai** : `429`, `503`, erreur réseau et timeout se réessaient, avec un back-off exponentiel de
  100 ms à 10 s avec gigue, remplacé par `Retry-After` quand le serveur le fournit.
- **Bail** : le client tient l'ensemble des jetons reçus et non réglés, et les renouvelle tous en un
  seul appel (§6.3), au tiers de `lease_ms` au plus. Il retire un jeton de cet ensemble **avant** de le
  régler : si le règlement échoue, le bail expire et le message est redistribué.
- **Indisponibilité** : pendant une indisponibilité, un `pull` du client C# **rend une liste vide** et
  reste sain ; il ne propage pas l'erreur au Pollster.
- **Redémarrage** : un changement d'`X-Broker-Epoch` est journalisé ; les tâches soumises avant doivent
  être reprises (pause puis reprise de leurs sessions). Les jetons de l'epoch précédente restent
  acquittables : ils reçoivent un succès silencieux.

## 6. Opérations

### 6.1 Enqueue — `POST /v1/partitions/{partition}/messages`

Lot homogène : partition, clé et priorité dans l'en-tête. La partition est créée si elle n'existe pas.

```json
{ "key": "session-42", "priority": 5,
  "items": [ { "task_id": "0f8c…###1" },
             { "task_id": "7a1e…",
               "affinity": { "hashes": [123, 456], "sizes": [40, 12], "dep_count": 2, "total_size": 41 } } ] }
```

- `items` : 1 à `max_batch_items` éléments (dérivé de `max_body_bytes`, ≈ 150 par défaut). Au-delà, ou si
  le corps dépasse `max_body_bytes` : `413`, et le client découpe le lot en deux puis renvoie chaque moitié.
- `affinity` (facultatif) : `hashes` et `sizes` de même longueur, au plus 8 (§8.1) ;
  `dep_count` : nombre total de dépendances, saturé à 65535 ; `total_size` : taille encodée de la somme des tailles.
- `delay_ms` (facultatif, sur l'en-tête) : visibilité différée du lot, au plus 24 h.

Réponse `200` :

```json
{ "accepted": 2, "occupancy": "normal" }
```

`occupancy` vaut `normal` ou `high` (seuil mémoire souple dépassé, indicatif).

### 6.2 Pull — `POST /v1/partitions/{partition}/pull`

```json
{ "max": 1, "wait_ms": 600000,
  "node": { "id": "node-17", "cache_capacity_bytes": 10737418240,
            "fetch_fixed_cost_us": 3000, "fetch_throughput_bytes_per_s": 1000000000 } }
```

- `max` : au moins 1, **plafonné** par `max_pull` (64 par défaut) ; `0` produit `400`.
- `wait_ms` : **plafonné** par `max_wait_ms` (10 min par défaut).
- `node` et chacun de ses champs sont facultatifs ; sans `node.id`, l'affinité est inactive pour ce
  pull. Le serveur retient la dernière déclaration de chaque noeud, et l'oublie après
  `node_forget_ms` sans pull de sa part.
- La partition est créée si elle n'existe pas.
- Retour **partiel** dès qu'au moins un message est disponible.

Réponse `200` :

```json
{ "lease_ms": 30000,
  "messages": [ { "token": "AAAB…", "task_id": "0f8c…###1", "attempts": 1 } ] }
```

Chaque message distribué a son propre bail de `lease_ms` à partir de sa distribution, prolongé seulement
par un renouvellement qui le nomme (§6.3). Une rupture de connexion ne remet rien en file : seul le bail
fait foi.

`204` sans corps si l'attente expire. À l'arrêt propre du broker, les attentes reçoivent `204`.

### 6.3 Renouvellement — `POST /v1/renew`

```json
{ "tokens": [ "AAAB…", "AAAC…" ] }
```

Prolonge de `lease_ms` le bail des messages **nommés**, et d'eux seuls, en un seul appel pour tous les
messages que le client détient, quelle que soit leur partition. Un message qu'il ne nomme pas n'est pas
renouvelé et revient en file à l'expiration de son bail : c'est ce qui récupère une réponse de pull
perdue en route ou un règlement abandonné. Réponse `200` :

```json
{ "lease_ms": 30000, "unknown": [ "AAAC…" ] }
```

`unknown` liste les jetons qui ne désignent plus une distribution en cours (déjà réglés, expirés, d'une
autre epoch) : le client cesse de les renouveler. Un jeton illisible produit `400`.

### 6.4 Ack — `POST /v1/ack`

```json
{ "items": [ { "token": "AAAB…", "outputs": { "hashes": [789], "sizes": [33] } } ] }
```

`outputs` (facultatif) : sorties produites, même règle de sélection que les dépendances (§8.1).
Réponse `200` : `{ "applied": 1, "ignored": 0 }`.

**Règle de sûreté.** Un jeton n'acquitte jamais un autre message que celui de sa distribution. Un jeton
bien formé qui ne désigne plus une distribution en cours — acquitté, redistribué, autre epoch, slot
inconnu — est **ignoré avec succès** et compté dans `ignored`. Seul un jeton illisible produit `400`.

### 6.5 Nack — `POST /v1/nack`

```json
{ "items": [ { "token": "AAAB…", "policy": "requeue" },
             { "token": "AAAC…", "policy": "delay", "delay_ms": 5000 },
             { "token": "AAAD…", "policy": "backoff" } ] }
```

- `requeue` (défaut) : remise en file immédiate, en queue de sa priorité.
- `delay` : remise en file après `delay_ms` (au plus 24 h).
- `backoff` : délai `min(backoff_base_ms × 2^(attempts-1), backoff_max_ms)`, soit 1 s × 2^n plafonné à 60 s par défaut.

Même règle de sûreté que l'ack. Réponse `200` : `{ "applied": n, "ignored": m }`.

### 6.6 Administration et diagnostic

| Route | Réponse |
|---|---|
| `GET /v1/partitions/{p}/stats?top=N&key=K` | `{ "ready": n, "in_flight": n, "delayed": n, "oldest_ready_age_ms": n, "waiters": n, "keys": [ { "key": "…", "ready": n, "in_flight": n, "oldest_ready_age_ms": n } ] }` — `keys` : la clé `K` si fournie, sinon les `N` plus grosses (défaut 10) |
| `GET /v1/partitions/{p}/leases?limit=N` | `{ "leases": [ { "token", "task_id", "node_id", "dispatched_age_ms", "attempts" } ] }`, des plus anciennement distribués aux plus récents |
| `GET /v1/partitions/{p}/peek` | `{ "heads": [ { "key", "priority", "task_id" } ] }` : tête de chaque couple clé et priorité non vide, au plus 1000 |
| `GET /v1/messages/{token}` | `{ "state": "in-flight", "task_id", "partition", "node_id", "dispatched_age_ms", "attempts" }` ou `{ "state": "stale" }` |
| `DELETE /v1/partitions/{p}` | `204` ; supprime messages, baux et clés ; les pulls en attente reçoivent `204` et les jetons émis deviennent périmés |
| `GET /v1/health` | `200 { "status": "ok" }`, ou `503` à l'arrêt |
| `GET /metrics` | format texte Prometheus |

Une partition inconnue en lecture renvoie des compteurs nuls, pas `404`.

## 7. Jeton

Opaque pour le client. Le serveur y code `epoch`, `partition`, `slot` et `génération` et le valide
entièrement ; la génération change à chaque sortie de l'état en vol. La forme actuelle (base64url de
16 octets) n'est pas contractuelle.

## 8. Structure d'affinité

Calculée par le producteur (Core) ; le broker ne voit que le résultat. Les vecteurs de
`conformance/affinity.json` fixent le comportement attendu ; Rust et C# doivent les reproduire à l'identique.

### 8.1 Sélection des 8 emplacements

Entrée : liste de couples (identifiant, taille en octets). Les identifiants en double sont fusionnés
(on garde la première taille).

1. Calculer `h = hash(id)` (§8.2) pour chaque dépendance.
2. **Moitié par taille** : trier par taille décroissante, puis par `h` croissant, puis par identifiant
   (ordinal) croissant ; prendre les 4 premiers.
3. **Moitié par hachage** : parmi les dépendances restantes, trier par `h` croissant, puis par
   identifiant croissant ; compléter jusqu'à 8 au total.
4. Émettre dans l'ordre : moitié par taille, puis moitié par hachage. `sizes[i] = encode(taille)` (§8.3),
   jamais 0 pour une dépendance réelle.
5. `dep_count = min(nombre de dépendances distinctes, 65535)` ;
   `total_size = encode(somme des tailles, saturée à 2^64-1)`.

Aucune dépendance : `affinity` absent.

### 8.2 Hachage

```
fnv1a64(bytes):  h = 0xcbf29ce484222325 ; pour chaque octet b : h = (h XOR b) * 0x100000001b3   (mod 2^64)
fmix64(k):       k ^= k >> 33 ; k *= 0xff51afd7ed558ccd ; k ^= k >> 33 ; k *= 0xc4ceb9fe1a85ec53 ; k ^= k >> 33
hash(id)       = (fmix64(fnv1a64(utf8(id))) mod 2^32)
```

Le mélange final garantit que des identifiants proches (UUID v7 horodatés, compteurs) ne produisent pas de
hachages proches.

### 8.3 Encodage logarithmique des tailles

Six pas par octave (≈ 12,2 % par pas), sur la valeur `v = s + 1` ; calcul entier uniquement.

```
T = [ 4294967296, 4820937788, 5411319705, 6074001000, 6817835604, 7652761717 ]   // round(2^32 · 2^(k/6))
encode(s):
    v = s + 1                      (saturé à 2^64 - 1)
    e = 63 - leading_zeros(v)      // floor(log2 v)
    f = (v · 2^32) >> e            // calcul sur 128 bits ; f ∈ [2^32, 2^33)
    k = nombre d'éléments de T inférieurs ou égaux à f   (1 à 6)
    return min(255, 6·e + k)
decode(c) ≈ 2^((c - 1) / 6) - 1    // usage indicatif (scoring), jamais comparé entre implémentations
```

`encode(0) = 1`, `encode(1) = 7`, encodage croissant au sens large ; 255 est atteint vers 2^42 octets (≈ 4 Tio).
