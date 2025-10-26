# Metadata about occupation

To survey participant's `occupation`, the [ISTAT (Italian National Statistical Institute)](https://www.istat.it/en/) classification was employed.
[ISTAT classification](https://professioni.istat.it/) comprises 8 categories, you can see how labels were translated below. For the translation we applied as much as possible [ISCO terminology](https://isco.ilo.org/en/).

We added an additional level (prefixed by 0) to the 9-level classification provided by ISTAT, as our dataset also includes retired and unemployed people, and students.

| ISTAT category                                                                          | occupation label                    |
| --------------------------------------------------------------------------------------- | ----------------------------------- |
| -                                                                                       | 0-Retired, 0-Students, 0-Unemployed |
| 1 - LEGISLATORI, IMPRENDITORI E ALTA DIRIGENZA                                          | 1-Managers                          |
| 2 - PROFESSIONI INTELLETTUALI, SCIENTIFICHE E DI ELEVATA SPECIALIZZAZIONE               | 2-Professionals                     |
| 3 - PROFESSIONI TECNICHE                                                                | 3-Technicians                       |
| 4 - PROFESSIONI ESECUTIVE NEL LAVORO D'UFFICIO                                          | 4-Clerical-Workers                  |
| 5 - PROFESSIONI QUALIFICATE NELLE ATTIVITÀ COMMERCIALI E NEI SERVIZI                    | 5-Service-Workers                   |
| 6 - ARTIGIANI, OPERAI SPECIALIZZATI E AGRICOLTORI                                       | 6-Craft-Workers                     |
| 7 - CONDUTTORI DI IMPIANTI, OPERAI DI MACCHINARI FISSI E MOBILI E CONDUCENTI DI VEICOLI | 7-Plant-Operators                   |
| 8 - PROFESSIONI NON QUALIFICATE                                                         | 8-Elementary                        |
| 9 - FORZE ARMATE                                                                        | 9-Armed-Forces                      |