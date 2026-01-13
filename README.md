# WhatsApp Text2SQL Financial Assistant
---

## 🧠 Présentation du projet

L’objectif de ce projet est de concevoir un **assistant WhatsApp** capable de transformer des requêtes en langage naturel (**français et anglais**) en requêtes **SQL PostgreSQL**, tout en respectant les contraintes suivantes :

- Confidentialité des données financières  
- Gouvernance des accès selon les rôles utilisateurs  
- Exécution sécurisée et validée des requêtes SQL  

Le cœur du système repose sur un module **Text2SQL** utilisant un **LLM open source exécuté localement**, garantissant :

- L’absence de dépendance à une API commerciale  
- La reproductibilité académique  
- La sécurité des données

---

## 🎯 Objectifs pédagogiques

- Concevoir une architecture **micro-services** dockerisée  
- Implémenter un pipeline **Text2SQL** robuste  
- Gérer des **rôles utilisateurs** (administrateur, responsable de projet, lecture seule)  
- Intégrer un **LLM open source** local  
- Appliquer des mécanismes de **sécurisation SQL**  
- Préparer une intégration **WhatsApp (Twilio)**

---

## 🧱 Architecture du système

### 🔹 Architecture logique

```

Utilisateur WhatsApp
│
▼
API Gateway (FastAPI)
│
▼
Text2SQL Service (FastAPI)
│
▼
LLM Local (Ollama - Llama 3.1 8B)

```

La base de données **PostgreSQL** est isolée et accessible uniquement via un service dédié (à venir).

---

### 🔹 Architecture micro-services (Docker)

```

┌────────────────────┐
│  API Gateway       │  ← Webhook WhatsApp (Twilio)
│  (FastAPI)         │
└─────────┬──────────┘
│
┌─────────▼──────────┐
│  Text2SQL Service  │
│  (FastAPI)         │
└─────────┬──────────┘
│
┌─────────▼──────────┐
│  Ollama             │
│  Llama 3.1 8B       │
└────────────────────┘

┌────────────────────┐
│ PostgreSQL          │
│ Base financière     │
└────────────────────┘

```

Chaque composant est isolé dans un conteneur Docker, facilitant :

- Maintenance  
- Tests  
- Déploiement  
- Évolutivité

---

## 🧠 Choix du LLM : **Llama 3.1 8B Instruct**

### 🔹 Raisons techniques

- Support bilingue FR / EN  
- Capacité à suivre des instructions strictes  
- Sorties SQL contrôlables et fiables  
- Modèle open source exécutable localement  
- Compatible avec un déploiement **on-premise**

### 🔹 Justification académique

L’utilisation d’un LLM open source permet :

- D’éviter les contraintes de facturation et de quota  
- De garantir la confidentialité des données financières  
- D’assurer la reproductibilité des expériences  
- De maîtriser l’ensemble de la chaîne de traitement
---

## 🧩 Services implémentés

### ✅ PostgreSQL
- Base de données financière  
- Initialisation automatique via `db/init.sql`  
- Données persistées via volume Docker  

### ✅ Ollama
- Serveur local de LLM  
- Modèle utilisé : **Llama 3.1 8B Instruct**  
- API HTTP exposée sur le port `11434`  

### ✅ Text2SQL Service
- Service FastAPI  
- Génération de requêtes SQL à partir de texte libre  
- Support FR / EN  
- Sortie strictement SQL  
- Aucune dépendance à OpenAI  

### 🟡 API Gateway
- Préparée pour l’intégration WhatsApp (Twilio)  
- Orchestration des services  
- Point d’entrée unique du système  

---

## 🗂️ Structure du projet

```

whatsapp-text2sql-financial-assistant/
│
├── docker-compose.yml
├── .env
│
├── db/
│   └── init.sql
│
├── services/
│   ├── api-gateway/
│   ├── text2sql/
│   │   ├── app.py
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── tests/
│   │       └── test_text2sql.py
│   └── ...
│
└── README.md

````

---

## ⚙️ Technologies utilisées

| Composant | Technologie |
|------------|------------|
| Backend    | Python 3.10 |
| API        | FastAPI |
| LLM        | Llama 3.1 8B Instruct |
| Runtime LLM| Ollama |
| Base de données | PostgreSQL |
| Conteneurisation | Docker / Docker Compose |
| Tests      | Python + requests |
| Messaging (à venir) | Twilio WhatsApp |

---

## 🚀 Lancer le projet

### 1️⃣ Prérequis
- Docker & Docker Compose  
- Python 3.10+  
- 16 Go de RAM recommandés  

### 2️⃣ Démarrage des services
```bash
docker-compose up -d --build
````

### 3️⃣ Téléchargement du modèle LLM

```bash
docker exec -it ollama ollama pull llama3.1:8b
```

Vérification :

```bash
docker exec -it ollama ollama list
```

---

## 🧪 Tests du module Text2SQL

Lancer le test automatisé :

```bash
python services/text2sql/tests/test_text2sql.py
```

### Exemple de requête

**Entrée :**

```
Show all expenses for project Alpha in 2025
```

**Sortie :**

```sql
SELECT * FROM expenses WHERE project = 'Alpha' AND year = 2025;
```

---

## 🔐 Sécurité (en cours de développement)

* Le module Text2SQL **ne se connecte pas directement à la base**
* Un service `sql-guard` sera ajouté pour :

  * Bloquer les requêtes dangereuses (`DROP`, `DELETE`, `ALTER`)
  * Appliquer les règles de rôles utilisateurs
* Un service `sql-executor` dédié exécutera les requêtes validées

---

## 📲 Intégration WhatsApp (à venir)

* Connexion via Twilio
* Messages WhatsApp → API Gateway → Text2SQL
* Réponse SQL formatée et lisible

---

## 📌 État d’avancement

| Fonctionnalité    | Statut |
| ----------------- | ------ |
| PostgreSQL        | ✅      |
| LLM local (Llama) | ✅      |
| Text2SQL          | ✅      |
| Tests automatisés | ✅      |
| SQL Guard         | ⏳      |
| SQL Executor      | ⏳      |
| WhatsApp (Twilio) | ⏳      |
| Audit & logs      | ⏳      |

---
