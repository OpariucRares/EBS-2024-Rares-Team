## Technical report
Setup:

- Physical machine hardware: AMD Ryzen 7 7840HS with Radeon 780M Graphics.
- Virtual machine hardware: 4 CPUs, 16GB RAM.
- Number of publications: up to 10000 publications (available to be transmitted).
- Number of subsctipions:
  - Subscriber 1: 5000 subscriptions.
  - Subscriber 2: 5000 subscriptions.
 
- Feed time: 3 minutes.
- Subscription parameters:
  - Field distribution: company: 50%, value: 20%, drop: 10%, variation: 10%, date: 10%.
  - Equality operator probability on company field: 25% vs. 100%.
 
**Results**:

| Metric                                | 25% equality on company       | 100% equality on company             |
|---------------------------------------|-------------------------------|--------------------------------------|
| Publications transmitted successfully | 100% (171 out of 171)         | 100% (171 out of 171)                |
| Mean latency                          | 18 / 24 ms                    | 15 / 31 ms                           |
| Matching rate                         | 99.41% (170 / 170 out of 171) | 97.66% / 100% (167 / 171 out of 171) |


## Structura
  Acest repository este pentru materia Event Based Systems. Acesta este impartit in doua foldere: Tema Practica si Proiect.

## Enunt Tema Practica
Scrieti un program care sa genereze aleator seturi echilibrate de subscriptii si publicatii cu posibilitatea de fixare a: numarului total de mesaje (publicatii, respectiv subscriptii), ponderii pe frecventa campurilor din subscriptii si ponderii operatorilor de egalitate din subscriptii pentru cel putin un camp. Publicatiile vor avea o structura fixa de campuri. Implementarea temei va include o posibilitate de paralelizare pentru eficientizarea generarii subscriptiilor si publicatiilor, si o evaluare a timpilor obtinuti.

Exemplu:
Publicatie: {(company,"Google");(value,90.0);(drop,10.0);(variation,0.73);(date,2.02.2022)} - Structura fixa a campurilor publicatiei e: company-string, value-double, drop-double, variation-double, date-data; pentru anumite campuri (company, date), se pot folosi seturi de valori prestabilite de unde se va alege una la intamplare; pentru celelalte campuri se pot stabili limite inferioare si superioare intre care se va alege una la intamplare.

Subscriptie:{(company,=,"Google");(value,>=,90);(variation,<,0.8)} - Unele campuri pot lipsi; frecventa campurilor prezente trebuie sa fie configurabila (ex. 90% company - 90% din subscriptiile generate trebuie sa includa campul "company"); pentru cel putin un camp (exemplu - company) trebui sa se poate configura un minim de frecventa pentru operatorul "=" (ex. macar 70% din subscriptiile generate sa aiba ca operator pe acest camp egalitatea).

Note:
- cazul in care suma procentelor configurate pentru campuri e mai mica decat 100 reprezinta o situatie de exceptie care nu e necesar sa fie tratata (pentru testare se vor folosi intotdeauna valori de procentaj ce sunt egale sau depasesc 100 ca suma)
- tema cere doar generarea de date, nu implementarea unei topologii Storm care sa includa functionalitatea de matching intre subscriptii si publicatii; nu exista restrictii de limbaj sau platforma pentru implementare
- pentru optimizarea de performanta prin paralelizare pot fi considerate fie threaduri (preferabil) sau o rulare multiproces pentru generarea subscriptiilor si publicatiilor; se va lua in calcul posibila necesitate de sincronizare a implementarii paralelizate ce poate sa apara in functie de algoritmul ales pentru generarea subscriptiilor
- evaluarea implementarii va preciza intr-un fisier "readme" asociat temei urmatoarele informatii: tipul de paralelizare (threads/procese), factorul de paralelism (nr. de threads/procese) - se cere executia pentru macar doua valori de test comparativ, ex. 1 (fara paralelizare) si 4, numarul de mesaje generat, timpii obtinuti si specificatiile procesorului pe care s-a rulat testul.

Hint: NU se recomanda utilizarea distributiei random in obtinerea procentelor cerute pentru campurile subscriptiilor (nu garanteaza o distributie precisa).

Setul generat va fi memorat in fisiere text. Tema se poate realiza in echipe de pana la 3 studenti.

## Enunt Proiect
Tema proiect si criterii evaluare:
Implementati o arhitectura de sistem publish/subscribe, content-based, structurata in felul urmator:

- Generati un flux de publicatii care sa fie emis de 1-2 noduri publisher. Publicatiile pot fi generate cu valori aleatoare pentru campuri folosind generatorul de date din tema practica. (5 puncte)
- Implementati o retea (overlay) de 2-3 brokeri care sa stocheze subscriptii primite de la clienti (subscriberi) si sa-i notifice pe acestia in functie de o filtrare bazata pe continutul publicatiilor. (10 puncte)
- Simulati 2-3 noduri subscriber care se conecteaza aleatoriu la reteaua de brokeri pentru a inregistra susbcriptii. Subscriptiile pot fi generate cu valori aleatoare pentru campuri folosind generatorul de date din tema practica. (5 puncte)
- Implementati un mecanism avansat de rutare la inregistrarea subscriptiilor. Subscriptiile aceluiasi subscriber vor fi distribuite balansat pe mai multi brokeri fiind rutate conform mecanismului implementat. Publicatiile vor trece prin mai multi brokeri pana la destinatie, fiecare ocupandu-se partial de rutarea acestora, si nu doar unul care contine toate subscriptiile si face un simplu match. (5 puncte)
- Realizati o evaluare a sistemului, masurand pentru inregistrarea a 10000 de subscriptii simple, urmatoarele statistici: a) cate publicatii se livreaza cu succes prin reteaua de brokeri intr-un interval continuu de feed de 3 minute, b) latenta medie de livrare a unei publicatii (timpul de la emitere pana la primire) pentru acelasi interval, c) rata de potrivire (matching) pentru cazul in care subscriptiile contin pe unul dintre campuri doar operator de egalitate (100%) comparata cu situatia in care frecventa operatorului de egalitate pe campul respectiv este aproximativ un sfert (25%). Redactati un scurt raport de evaluare a solutiei. (10 puncte)

### Punctaj bonus proiect:

- Folositi un mecanism de serializare binara (Google Protocol Buffers sau Thrift) pentru transmiterea publicatiilor de la nodul publisher la brokers. (5 puncte)
- Simulati si tratati (prin asigurare de suport in implementare) cazuri de caderi pe nodurile broker, care sa asigure ca nu se pierd notificari. Simularea va presupune intreruperea efectiva a functionarii unui nod broker. (5 puncte)
- Implementati o modalitate de filtrare a mesajelor care sa nu permita accesul la continutul acestora (match pe subscriptii/publicatii criptate). (5-10 puncte)
Note:
Proiectul poate fi realizat in echipe de pana la 3 studenti si va fi prezentat la o data ce va fi stabilita in perioada de sesiune.
Proiectul poate fi implementat utilizand orice limbaj sau platforma. In cazul in care se va folosi Apache Kafka in implementare, utilizarea acestei platforme va fi limitata doar pentru livrarea mesajelor, asigurandu-se conectarea cu implementarea folosind o alta solutie pentru partea efectiva de serviciu de procesare a datelor (filtrarea bazata pe continut, stocare subscriptii, etc).
