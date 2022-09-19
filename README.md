# inexistent_sitelinks
Wikidata bot that handles sitelinks to pages that do not exist anymore.

While Wikibase should usually manage sitelinks automatically, there are some scenarios where this fails and a sitelink remains in the Wikibase repository (Wikidata) although the page on the client wiki does not exist any longer (e.g. due to a deletion or page move). This bot finds these sitelinks and repairs them, either by removal or update.

There is a related ticket at Wikimedia Phabricator: [T143486](https://phabricator.wikimedia.org/T143486).

This is still work in progress. The code has largely been developed at [PAWS](https://hub.paws.wmcloud.org/user/MisterSynergy/lab/tree/misc/2021%2012%20deleted%20sitelinks) (Jupyter notebook instance on Wikimedia servers) and an initial cleanup of ~60.000 sitelinks has been done there. The code should now be deployed to [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) and run regularly in order to keep the backlog short. An issue to deal with is the memory constraint found both on PAWS and Toolforge, which is why some evaluations have been done locally on MisterSynergy's personal machine until now.