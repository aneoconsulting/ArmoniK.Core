# ArmoniK Broker — protocole v1

Contrat entre le serveur `Broker/` (Rust) et son client `Adaptors/Broker/` (C#).
Il n'y a pas de génération de code : ce document et les vecteurs de `Broker/conformance/` sont
la seule source de vérité. La conception est décrite dans `__docs__/broker-armonik-architecture-v0.49.md`.

## 1. Transport

- HTTP/1.1 et HTTP/2 (h2 via ALPN, ou h2c en clair pour le développement). Même API.
- TLS optionnel ; mTLS quand une autorité cliente est configurée. Aucune autorisation par opération.
- Corps en `application/json`, UTF-8. Tailles de corps plafonnées par `max_body_bytes` (64 Kio par défaut).
- Toute réponse porte l'en-tête **`X-Broker-Epoch`** : entier non signé 32 bits en décimal, tiré au
  hasard au démarrage. Son changement signifie que le contenu de la file a été perdu.

## 2. Version

Toutes les routes sont préfixées par `/v1`. Une seule version est servie à la fois. Une requête vers un
autre préfixe `/vN` reçoit `404` avec le type `unsupported-version` : le client doit **attendre et
réessayer** (back-off), pas boucler — c'est le cas transitoire d'une mise à jour.

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
| Identifiant de consommateur | chaîne opaque |

Champs inconnus : ignorés. Champs obligatoires absents ou mal typés : `400 malformed`.

## 4. Erreurs

Corps `application/problem+json` (RFC 9457) :

```json
{ "type": "urn:armonik:broker:backpressure", "title": "backpressure", "status": 429,
  "detail": "hard memory threshold reached", "retryable": true }
```

| Type (`urn:armonik:broker:…`) | Statut | Réessayable | Signification | Conduite du client |
|---|---|---|---|---|
| `malformed` | 400 | non | corps invalide, jeton illisible, valeur hors format | bug client : journaliser, ne pas réessayer |
| `unsupported-version` | 404 | oui | préfixe de version non servi | attendre, back-off plafonné |
| `not-found` | 404 | non | route inconnue | bug client |
| `invalid-priority` | 409 | non | priorité hors de 1 à 16 | rejeter la soumission |
| `partition-limit` | 409 | oui | nombre maximal de partitions atteint | back-off |
| `unknown-consumer` | 410 | oui | identifiant de consommateur inconnu ou expiré (cas normal après redémarrage) | se réenregistrer puis rejouer |
| `payload-too-large` | 413 | non | corps au-delà de `max_body_bytes` | découper le lot (§6.1) |
| `backpressure` | 429 | oui | seuil mémoire dur, réserve de clés pleine | attendre `Retry-After`, rejouer le lot entier |
| `overloaded` | 503 | oui | anneau d'acteur plein, requêtes concurrentes en excès | attendre `Retry-After` |
| `shutting-down` | 503 | oui | arrêt en cours | attendre, se réenregistrer à la reprise |

`429` et `503` portent `Retry-After` (secondes). Un statut d'erreur est définitif : **rien n'a été
appliqué**, un rejeu ne crée pas de doublon. Le lot d'enqueue est atomique.

## 5. Machine à états du client

```
         register ok                 410 unknown-consumer
 [Init] -----------> [Registered] ------------------------> [Init]
    ^                   |  429/503 : back-off avec gigue, rejouer
    |  erreur réseau,   |  404 unsupported-version : back-off, rejouer
    +-- 503 shutting ---+  changement d'epoch : journaliser, continuer
```

- Back-off : exponentiel de 100 ms à 10 s, gigue uniforme ±50 %, respect de `Retry-After`.
- Pendant une indisponibilité, un `pull` du client C# **rend une liste vide** et reste sain ; il ne
  propage pas l'erreur au Pollster.
- Les jetons obtenus avant un redémarrage restent acquittables : ils reçoivent un succès silencieux.

## 6. Opérations

### 6.1 Enqueue — `POST /v1/partitions/{partition}/messages`

Lot homogène : partition, clé et priorité dans l'en-tête. La partition est créée si elle n'existe pas.

```json
{ "key": "session-42", "priority": 5,
  "items": [ { "task_id": "0f8c…###1" },
             { "task_id": "7a1e…",
               "affinity": { "hashes": [123, 456], "sizes": [40, 12], "dep_count": 2, "total_size": 41 } } ] }
```

- `items` : 1 à `max_batch_items` éléments (dérivé de `max_body_bytes`, ≈ 150 par défaut, publié par `GET /v1/limits`). Au-delà : 413.
- `affinity` (facultatif, jalon 4) : `hashes` et `sizes` de même longueur, au plus 8 (§8.1) ;
  `dep_count` : nombre total de dépendances, saturé à 65535 ; `total_size` : taille encodée de la somme des tailles.
- `delay_ms` (facultatif, sur l'en-tête) : visibilité différée du lot, au plus 24 h.

Réponse `200` :

```json
{ "accepted": 2, "occupancy": "normal" }
```

`occupancy` vaut `normal` ou `high` (seuil mémoire souple dépassé, indicatif).

### 6.2 Enregistrement — `POST /v1/consumers`

```json
{ "partition": "default",
  "node": { "id": "node-17", "cache_capacity_bytes": 10737418240,
            "fetch_fixed_cost_us": 3000, "fetch_throughput_bytes_per_s": 1000000000 } }
```

`node` et chacun de ses champs sont facultatifs ; sans `node.id`, l'affinité est inactive pour ce
consommateur. Réponse `200` :

```json
{ "consumer_id": "c-3f2a9b10", "lease_ms": 30000, "grace_ms": 30000 }
```

L'enregistrement vaut 2 h glissantes, prolongées par tout échange. Chaque message distribué a son
propre bail de `lease_ms` à partir de sa distribution, prolongé seulement par un `renew` qui le nomme
(§6.4). Une rupture de connexion ne remet rien en file : seul le bail fait foi, de sorte que `grace_ms`
vaut aujourd'hui `lease_ms`. Le client doit renouveler nettement plus souvent que `lease_ms`, par
exemple au tiers.
L'identifiant porte l'epoch : après un redémarrage il est inconnu (`410`), jamais confondu avec un
nouveau consommateur.

`DELETE /v1/consumers/{id}` : désenregistrement propre ; ses messages en vol sont remis en file
immédiatement. `204`.

### 6.3 Pull — `POST /v1/consumers/{id}/pull`

```json
{ "max": 1, "wait_ms": 600000 }
```

- `max` : 1 à `max_pull` (64 par défaut, publié par `GET /v1/limits`). `wait_ms` : plafonné par `max_wait_ms` (10 min par défaut).
- Retour **partiel** dès qu'au moins un message est disponible.

Réponse `200` :

```json
{ "messages": [ { "token": "AAAB…", "task_id": "0f8c…###1", "attempts": 1 } ] }
```

`204` sans corps si l'attente expire. À l'arrêt propre du broker, les attentes reçoivent `204`.

### 6.4 Renouvellement — `POST /v1/consumers/{id}/renew`

```json
{ "tokens": [ "AAAB…", "AAAC…" ] }
```

Prolonge de `lease_ms` le bail des messages **nommés**, et d'eux seuls, en un seul appel pour tous les
messages que le consommateur détient. Un message qu'il ne nomme pas n'est pas renouvelé et revient en
file à l'expiration de son bail : c'est ce qui récupère une réponse de pull perdue en route ou un
règlement abandonné. Le corps peut être vide ou absent : l'appel ne fait alors que prolonger
l'enregistrement. Réponse `200` :

```json
{ "lease_ms": 30000, "unknown": [ "AAAC…" ] }
```

`unknown` liste les jetons qui ne désignent plus une distribution en cours chez ce consommateur (déjà
réglés, expirés, d'une autre epoch) : le client cesse de les renouveler. Un jeton illisible produit `400`.

### 6.5 Ack — `POST /v1/consumers/{id}/ack`

```json
{ "items": [ { "token": "AAAB…", "outputs": { "hashes": [789], "sizes": [33] } } ] }
```

`outputs` (facultatif, jalon 4) : sorties produites, même règle de sélection que les dépendances (§8.1).
Réponse `200` : `{ "applied": 1, "ignored": 0 }`.

**Règle de sûreté.** Un jeton n'acquitte jamais un autre message que celui de sa distribution. Un jeton
bien formé qui ne désigne plus une distribution en cours — acquitté, redistribué, autre epoch, slot
inconnu — est **ignoré avec succès** et compté dans `ignored`. Seul un jeton illisible produit `400`.

### 6.6 Nack — `POST /v1/consumers/{id}/nack`

```json
{ "items": [ { "token": "AAAB…", "policy": "requeue" },
             { "token": "AAAC…", "policy": "delay", "delay_ms": 5000 },
             { "token": "AAAD…", "policy": "backoff" } ] }
```

- `requeue` (défaut) : remise en file immédiate, en queue de sa priorité.
- `delay` : remise en file après `delay_ms` (au plus 24 h).
- `backoff` : délai `min(backoff_base_ms × 2^(attempts-1), backoff_max_ms)`, soit 1 s × 2^n plafonné à 60 s par défaut.

Même règle de sûreté que l'ack. Réponse `200` : `{ "applied": n, "ignored": m }`.

### 6.7 Administration et diagnostic

| Route | Réponse |
|---|---|
| `GET /v1/partitions/{p}/stats?top=N&key=K` | `{ "ready": n, "in_flight": n, "delayed": n, "oldest_ready_age_ms": n, "consumers": n, "keys": [ { "key": "…", "ready": n, "in_flight": n, "oldest_ready_age_ms": n } ] }` — `keys` : la clé `K` si fournie, sinon les `N` plus grosses (défaut 10) |
| `GET /v1/partitions/{p}/leases?limit=N` | `{ "leases": [ { "token", "task_id", "consumer_id", "node_id", "dispatched_age_ms", "attempts" } ] }`, des plus anciennement distribués aux plus récents |
| `GET /v1/partitions/{p}/peek` | `{ "heads": [ { "key", "priority", "task_id" } ] }` : tête de chaque couple clé et priorité non vide, au plus 1000 |
| `GET /v1/messages/{token}` | `{ "state": "in-flight", "task_id", "partition", "consumer_id", "node_id", "dispatched_age_ms", "attempts" }` ou `{ "state": "stale" }` |
| `DELETE /v1/partitions/{p}` | `204` ; supprime messages, baux et clés ; les consommateurs abonnés reçoivent ensuite `410` |
| `GET /v1/health` | `200 { "status": "ok" }`, ou `503` à l'arrêt |
| `GET /v1/limits` | `{ "lease_ms": n, "max_pull": n, "max_batch_items": n, "max_wait_ms": n }` : limites que le client doit respecter. Le client les lit au démarrage, producteurs compris puisqu'ils ne s'enregistrent pas, puis à chaque changement d'epoch, un serveur redémarré pouvant avoir une autre configuration |
| `GET /metrics` | format texte Prometheus |

Une partition inconnue en lecture renvoie des compteurs nuls, pas `404`.

## 7. Jeton

Opaque pour le client. Le serveur y code `epoch`, `partition`, `slot` et `génération` et le valide
entièrement ; la génération change à chaque sortie de l'état en vol. La forme actuelle (base64url de
16 octets) n'est pas contractuelle.

## 8. Structure d'affinité (jalon 4)

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
