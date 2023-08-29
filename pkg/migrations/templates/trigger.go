package templates

const Trigger = `CREATE OR REPLACE TRIGGER {{ .Name | qi }}
    BEFORE UPDATE OR INSERT
    ON {{ .TableName | qi }}
    FOR EACH ROW
    EXECUTE PROCEDURE {{ .Name | qi }}();
`
