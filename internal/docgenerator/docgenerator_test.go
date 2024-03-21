package docgenerator

import (
	"log"

	"testing"

	"github.com/AryaanB9/sirius_aryaan/internal/template"
	"github.com/bgadrian/fastfaker/faker"
)

func TestGenerator_GetNextKey(t *testing.T) {

	temp := template.InitialiseTemplate("person")
	seed := int64(1678383842563225000)

	g := &Generator{
		Template: temp,
		KeySize:  128,
	}
	for i := int64(0); i < int64(10); i++ {
		key := seed + i
		docId := g.BuildKey(key)
		log.Println(docId)
		fake := faker.NewFastFaker()
		fake.Seed(key)
		_ = g.Template.GenerateDocument(fake, docId, 1024)

	}

}
