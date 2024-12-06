INSERT INTO ready-de-25.surrogates.business_surrogate_keys (source_key,domain_id,edw_key)
with src_key AS
(
	select distinct coalesce(trim(tax_reg_no), '') source_key
	from ready-de-25.surrogates_source.company
)
, v_business_surrogate_keys as
(
	select source_key, edw_key,  d.dataset_id, sk.domain_id
	from  ready-de-25.surrogates.business_surrogate_keys sk, ready-de-25.surrogates.domains_registration d
	WHERE sk.domain_id = d.id
)

, last_edw_key as (select COALESCE(max(edw_key),0) max_edw_key from v_business_surrogate_keys where dataset_id = 1004)
select source_key, 60 domain_id, 
ROW_NUMBER() over(order by source_key) + k.max_edw_key edw_key

from src_key s, last_edw_key k
where not exists (select 1 from v_business_surrogate_keys t where t.source_key = s.source_key AND t.domain_id = 60);


----------------------------------------------------------------------------------------------------------------------------

INSERT INTO `ready-de-25.surrogates.party` (party_id, party_type_code,name)
with srci_customer AS
(
  SELECT sk.edw_key AS party_id, "c" AS party_type_code, c.name
  FROM ready-de-25.surrogates_source.customer c 
  LEFT JOIN ready-de-25.surrogates.business_surrogate_keys sk
  ON coalesce(trim(c.email), '') = sk.source_key
  AND sk.domain_id = 70
)

SELECT * from srci_customer



