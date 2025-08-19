# 🌳 PureFS

**PureFS est un système de stockage distribué conçu pour être simple et résilient.**  

Chaque fichier est toujours stocké en **deux exemplaires sur deux disques distincts**. Les disques peuvent être connectés sur n’importe quelle machine : il suffit de lancer un agent pour qu’ils soient intégrés au système.  

Cas 1 : Si deux copies ne peuvent pas être garanties lors d’un upload, l’opération est refusée.  
Cas 2 : Si un disque tombe en panne, les fichiers restés avec une seule copie sont automatiquement répliqués.  
Cas 3 : Si un disque est reconnecté, les doublons inutiles sont supprimés afin de conserver strictement deux copies par fichier.  

➡️ **Une interface ultra-simple, aucune configuration complexe, une tolérance aux pannes automatique.**


## 🖥️ Interface Web

Une fois le serveur démarré, ouvrez votre navigateur à l'adresse `http://localhost:8080`. Vous y trouverez le panneau de contrôle, qui est votre principal outil de gestion.

### 1. Comprendre l'affichage

L'écran est divisé en deux parties principales :

**À gauche : Les Volumes de Stockage**
Cette section liste tous les disques (ou plus précisément, les "volumes" de 30 Go) que le système connaît.

*   **Disque Physique** : Un identifiant unique pour un disque dur physique. Plusieurs volumes peuvent appartenir au même disque.
*   **Nom Volume** : Le nom unique du volume de 30 Go.
*   **Espace Libre** : L'espace restant sur ce volume spécifique.
*   **Statut** :
    *   <span style="color: #27ae60;">**● En ligne**</span> : Le volume est connecté, sain et prêt à être utilisé.
    *   <span style="color: #c0392b;">**● Hors ligne**</span> : L'agent de disque de ce volume ne répond plus. Les données qu'il contient sont temporairement inaccessibles, mais ne sont pas perdues.

**À droite : Les Fichiers Stockés**
C'est la liste de tous vos fichiers. La colonne la plus importante est **"Statut de Redondance"**.

*   <span style="color: #27ae60;">**✔ Protégé**</span> : **État idéal.** Le fichier possède le nombre requis de copies (par défaut 2) sur des disques physiques différents. Il est en sécurité.
*   <span style="color: #f39c12;">**⚠ Dégradé**</span> : **Attention.** Un disque est tombé en panne et une des copies du fichier est inaccessible. **Le fichier est toujours lisible** grâce à sa copie restante, mais il n'est plus protégé contre une nouvelle panne.
*   <span style="color: #c0392b;">**✖ Indisponible**</span> : **Critique.** Plusieurs disques contenant les copies du fichier sont hors ligne. Le système n'a plus accès à une copie valide du fichier, qui ne peut donc plus être téléchargé.
*   <span style="color: #3498db;">**ℹ️ Sur-protégé**</span> : **Information.** Le fichier a plus de copies que nécessaire. Cela peut arriver après une réparation si un disque revient en ligne. Ce n'est pas une erreur, mais une opportunité de libérer de l'espace.

### 2. Les Actions Concrètes (Les Boutons)

#### **Ajouter un nouveau fichier**
-   **Action :** Cliquez sur "Choisir un fichier", sélectionnez un fichier sur votre ordinateur, puis cliquez sur "Envoyer".
-   **Comment ça marche :** Le système découpe votre fichier en blocs de 8 Mo, puis écrit deux copies de chaque bloc sur deux disques physiques différents pour garantir la sécurité.
-   **Condition :** Le bouton "Envoyer" est désactivé si vous n'avez pas au moins 2 disques physiques en ligne, car le système ne peut pas garantir la redondance.

#### **Télécharger un fichier**
-   **Action :** Cliquez sur le bouton vert "Télécharger".
-   **Comment ça marche :** Le système lit les blocs du fichier depuis l'une des copies disponibles et vous les envoie. Si un disque est en panne, il utilisera automatiquement l'autre copie.
-   **Condition :** Le bouton est désactivé si le statut du fichier est "Indisponible".

#### **Supprimer un fichier**
-   **Action :** Cliquez sur le bouton rouge "Supprimer". Une confirmation vous sera demandée.
-   **Comment ça marche :** C'est une "suppression logique". Le fichier disparaît de la liste, mais **les données occupent toujours de l'espace disque**. C'est une sécurité pour permettre au processus de nettoyage (ci-dessous) de s'exécuter de manière contrôlée.

#### **Lancer le nettoyage (Garbage Collection)**
-   **Action :** Ce bloc apparaît dès qu'au moins un fichier a été supprimé. Cliquez sur le bouton violet "Lancer le nettoyage".
-   **Comment ça marche :** C'est la **suppression physique**. Le système parcourt tous les volumes, efface définitivement les données des fichiers supprimés et réorganise les fichiers volumes pour récupérer l'espace disque. **C'est cette action qui libère réellement de l'espace.**

#### **Effectuer une réparation**
-   **Action :** Ce bloc apparaît si un fichier est "Dégradé". Cliquez sur le bouton orange "Effectuer une réparation".
-   **Comment ça marche :** Le système identifie les blocs de fichiers qui n'ont plus qu'une seule copie. Il lit cette copie valide et en écrit une nouvelle sur un autre disque sain pour restaurer la protection. Le statut du fichier repasse à "Protégé".
-   **Condition :** Le bouton n'est activé que s'il y a un disque de destination disponible (un disque en ligne qui ne contient pas déjà l'autre copie du bloc).

#### **Nettoyer les copies en surplus**
-   **Action :** Ce bloc apparaît si un fichier est "Sur-protégé". Cliquez sur le bouton bleu "Nettoyer les copies en surplus".
-   **Comment ça marche :** Le système identifie les blocs qui ont plus de 2 copies et supprime les copies excédentaires pour libérer de l'espace.

## ⚙️ API REST

Pour une utilisation programmatique ou via des scripts, toutes les actions de l'interface web sont disponibles via une API REST simple.

#### `POST /api/files/upload`
Envoie un fichier. La requête doit être de type `multipart/form-data`.
```bash
curl -X POST -F "file=@/chemin/vers/mon/fichier.zip" http://localhost:8080/api/files/upload
```

#### `GET /api/files/download/{filename}`
Télécharge un fichier.
```bash
curl -o fichier_recu.zip http://localhost:8080/api/files/download/fichier.zip
```

#### `POST /api/files/delete/{filename}`
Marque un fichier pour suppression (suppression logique).
```bash
curl -X POST http://localhost:8080/api/files/delete/fichier.zip
```

#### `POST /api/gc`
Lance le processus de garbage collection pour libérer l'espace des fichiers marqués comme supprimés.
```bash
curl -X POST http://localhost:8080/api/gc
```

#### `POST /api/repair`
Lance le processus de réparation pour les fichiers dégradés.
```bash
curl -X POST http://localhost:8080/api/repair
```

#### `POST /api/cleanup_replicas`
Nettoie les copies de fichiers excédentaires.
```bash
curl -X POST http://localhost:8080/api/cleanup_replicas
```

### API Interne (Serveur <-> Agent)

Ces endpoints sont utilisés par les agents de disque pour communiquer avec le serveur et ne sont généralement pas appelés manuellement.

#### `POST /api/disk/register`
Utilisé par un agent de disque pour s'enregistrer ou envoyer un heartbeat au serveur. Le corps de la requête est un JSON contenant les informations du volume (`name`, `diskId`, `address`, `totalSpace`, `freeSpace`).

## 🚀 Démarrage Rapide

1.  **Lancer le Serveur Maître :**
    ```bash
    ./purefs_server
    ```
    Le serveur est maintenant accessible sur `http://localhost:8080`.

2.  **Lancer un Agent de Disque :**
    Lancez un agent pour chaque machine de stockage. Spécifiez les chemins des disques à utiliser et l'adresse du serveur. ⚠️ N’utilisez jamais deux emplacements différents sur le même disque dans votre commande, sinon le stockage sera mal calculé et pourrait être saturé.

    *Exemple pour une machine gérant deux disques :*
    ```bash
    ./purefs_agent \
      -disks="/mnt/stockage1,/mnt/stockage2" \
      -server="<IP_DU_SERVEUR>:8080" \
      -listen-addr="0.0.0.0:9000"
    ```
    L'agent va scanner les disques, créer les fichiers volumes (`.dat`) et s'enregistrer auprès du serveur. Les volumes apparaîtront alors dans l'interface web.
