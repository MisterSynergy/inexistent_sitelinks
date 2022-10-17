# inexistent_sitelinks
Wikidata bot that handles sitelinks to pages that do not exist anymore.

While Wikibase should usually manage sitelinks automatically, there are some scenarios where this fails and a sitelink remains in the Wikibase repository (Wikidata) although the page on the client wiki does not exist any longer (e.g. due to a deletion or page move). This bot finds these sitelinks and repairs them, either by removal or update.

There is a related ticket at Wikimedia Phabricator: [T143486](https://phabricator.wikimedia.org/T143486).

This project is still work in progress. The code has initially largely been developed at [PAWS](https://hub.paws.wmcloud.org/user/MisterSynergy/lab/tree/misc/2021%2012%20deleted%20sitelinks) (Jupyter notebook instance on Wikimedia servers) and an initial cleanup of ~60.000 sitelinks has been done there. A migration to [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) has unfortunately been complicated due to high memory demand of the initial implementation. Much of the code has thus been rewritten in order to reduce memory requirements. Meanwhile, the code only works on Toolforge where a weekly cronjob is run in order to keep the backlog short.