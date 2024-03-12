package template

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/bgadrian/fastfaker/faker"
)

type ProductRating struct {
	RatingValue  float64 `json:"rating_value,omitempty"`
	Performance  float64 `json:"performance,omitempty"`
	Utility      float64 `json:"usability,omitempty"`
	Pricing      float64 `json:"pricing,omitempty"`
	BuildQuality float64 `json:"build_quality,omitempty"`
}

type ProductReview struct {
	Date          string        `json:"date,omitempty"`
	Author        string        `json:"author,omitempty"`
	ProductRating ProductRating `json:"rating,omitempty"`
}

type Product struct {
	ID                string            `json:"id" bson:"_id"`
	ProductName       string            `json:"product_name,omitempty"`
	ProductLink       string            `json:"product_link,omitempty"`
	ProductFeatures   []string          `json:"product_features,omitempty"`
	ProductSpecs      map[string]string `json:"product_specs,omitempty"`
	ProductImageLinks []string          `json:"product_image_links,omitempty"`
	ProductReviews    []ProductReview   `json:"reviews,omitempty"`
	ProductCategory   []string          `json:"product_category,omitempty"`
	Price             float64           `json:"price,omitempty"`
	AvgRating         float64           `json:"avg_rating,omitempty"`
	NumSold           int64             `json:"num_sold,omitempty"`
	UploadDate        string            `json:"upload_date,omitempty"`
	Weight            float64           `json:"weight,omitempty"`
	Quantity          int64             `json:"quantity,omitempty"`
	SellerName        string            `json:"seller_name,omitempty"`
	SellerLocation    string            `json:"seller_location,omitempty"`
	SellerVerified    bool              `json:"seller_verified,omitempty"`
	TemplateType      string            `json:"template_type"`
	Mutated           float64           `json:"mutated"`
}

// buildReview generates the Review slice to be added into Product struct
/*
 * length defines the number of reviews to be added to Review slice
 * approximate size of 1 review is around 95bytes
 */
func buildProductReview(fake *faker.Faker, length int32) []ProductReview {
	var prodReview []ProductReview
	for i := 0; i < int(length); i++ {
		prodReview = append(prodReview, ProductReview{
			Date:   fake.DateStr(),
			Author: fake.Name(),
			ProductRating: ProductRating{
				RatingValue:  float64(fake.Int32Range(0, 10)),
				Performance:  float64(fake.Int32Range(0, 10)),
				Utility:      float64(fake.Int32Range(1, 10)),
				Pricing:      float64(fake.Int32Range(0, 10)),
				BuildQuality: float64(fake.Int32Range(0, 10)),
			},
		})
	}
	return prodReview
}

func buildProductCategory(fake *faker.Faker, length int32) []string {
	var prodCategory []string
	for i := 0; i < int(length); i++ {
		prodCategory = append(prodCategory, fake.VehicleType())
	}
	return prodCategory
}

func buildProductImageLinks(fake *faker.Faker, length int32) []string {
	var prodImgLinks []string
	for i := 0; i < int(length); i++ {
		prodImgLinks = append(prodImgLinks, fake.URL())
	}
	return prodImgLinks
}

func buildProductFeatures(fake *faker.Faker, length int32) []string {
	var prodFeatures []string
	for i := 0; i < int(length); i++ {
		prodFeatures = append(prodFeatures, fake.Sentence(100))
	}
	return prodFeatures
}

func buildProductSpecs(fake *faker.Faker, length int32) map[string]string {
	prodSpecs := make(map[string]string)
	for i := 0; i < int(length); i++ {
		prodSpecs[fake.CarMaker()] = fake.CarModel()
	}
	return prodSpecs
}

func (p *Product) GenerateDocument(fake *faker.Faker, key string, documentSize int) interface{} {
	product := &Product{
		ID:                key,
		ProductName:       fake.Name(),
		ProductLink:       fake.URL(),
		ProductFeatures:   buildProductFeatures(fake, fake.Int32Range(1, 3)),
		ProductSpecs:      buildProductSpecs(fake, fake.Int32Range(1, 3)),
		ProductImageLinks: buildProductImageLinks(fake, fake.Int32Range(1, 3)),
		ProductReviews:    buildProductReview(fake, fake.Int32Range(1, 3)),
		ProductCategory:   buildProductCategory(fake, fake.Int32Range(1, 3)),
		Price:             fake.Price(100, 400000),
		AvgRating:         fake.Float64Range(1, 5),
		NumSold:           fake.Int64Range(0, 50000),
		UploadDate:        fake.DateStr(),
		Weight:            fake.Float64Range(0.1, 5),
		Quantity:          fake.Int64Range(0, 50000),
		SellerName:        fake.BeerName(),
		SellerLocation:    fake.Address().City + ", " + fake.Address().Country,
		SellerVerified:    fake.Bool(),
		TemplateType:      "Product",
		Mutated:           MutatedPathDefaultValue,
	}
	currentDocSize := calculateSizeOfStruct(product)
	if (currentDocSize) < documentSize {
		remSize := documentSize - currentDocSize
		numOfReviews := int(remSize/(95*2)) + 1
		prodReview := buildProductReview(fake, int32(numOfReviews))
		product.ProductReviews = prodReview
	}
	return product
}

func (p *Product) UpdateDocument(fieldsToChange []string, lastUpdatedDocument interface{}, documentSize int,
	fake *faker.Faker) (interface{}, error) {

	product, ok := lastUpdatedDocument.(*Product)
	if !ok {
		return nil, fmt.Errorf("unable to decode last updated document to product template")
	}

	checkFields := make(map[string]struct{})
	for _, s := range fieldsToChange {
		checkFields[s] = struct{}{}
	}

	if _, ok := checkFields["product_name"]; ok || len(checkFields) == 0 {
		product.ProductName = fake.Name()
	}
	if _, ok := checkFields["product_link"]; ok || len(checkFields) == 0 {
		product.ProductLink = fake.URL()
	}
	if _, ok := checkFields["product_features"]; ok || len(checkFields) == 0 {
		product.ProductFeatures = buildProductFeatures(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_specs"]; ok || len(checkFields) == 0 {
		product.ProductSpecs = buildProductSpecs(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_image_links"]; ok || len(checkFields) == 0 {
		product.ProductImageLinks = buildProductImageLinks(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["reviews"]; ok || len(checkFields) == 0 {
		product.ProductReviews = buildProductReview(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["product_category"]; ok || len(checkFields) == 0 {
		product.ProductCategory = buildProductCategory(fake, fake.Int32Range(1, 3))
	}
	if _, ok := checkFields["price"]; ok || len(checkFields) == 0 {
		product.Price = fake.Price(100, 400000)
	}
	if _, ok := checkFields["avg_rating"]; ok || len(checkFields) == 0 {
		product.AvgRating = fake.Float64Range(1, 5)
	}
	if _, ok := checkFields["num_sold"]; ok || len(checkFields) == 0 {
		product.NumSold = fake.Int64Range(0, 50000)
	}
	if _, ok := checkFields["upload_date"]; ok || len(checkFields) == 0 {
		//product.UploadDate = fake.Date().Format("2006-01-02")
		product.UploadDate = fake.DateStr()
	}
	if _, ok := checkFields["weight"]; ok || len(checkFields) == 0 {
		product.Weight = fake.Float64Range(0.1, 5)
	}
	if _, ok := checkFields["quantity"]; ok || len(checkFields) == 0 {
		product.Quantity = fake.Int64Range(0, 50000)
	}
	if _, ok := checkFields["seller_name"]; ok || len(checkFields) == 0 {
		product.SellerName = fake.BeerName()
	}
	if _, ok := checkFields["seller_location"]; ok || len(checkFields) == 0 {
		product.SellerLocation = fake.Address().City + ", " + fake.Address().Country
	}
	if _, ok := checkFields["seller_verified"]; ok || len(checkFields) == 0 {
		product.SellerVerified = fake.Bool()
	}

	currentDocSize := calculateSizeOfStruct(product)
	log.Println("Size of doc before appends:", currentDocSize)
	if currentDocSize < documentSize {
		remSize := documentSize - currentDocSize
		numOfReviews := int(remSize/(95*2)) + 1
		prodReview := buildProductReview(fake, int32(numOfReviews))
		product.ProductReviews = prodReview
	}

	return product, nil
}

func (p *Product) Compare(document1 interface{}, document2 interface{}) (bool, error) {
	p1, ok := document1.(*Product)
	if !ok {
		return false, fmt.Errorf("unable to decode first document to product template")
	}
	p2, ok := document2.(*Product)
	if !ok {
		return false, fmt.Errorf("unable to decode second document to product template")
	}
	return reflect.DeepEqual(p1, p2), nil
}

func (p *Product) GenerateIndexes(bucketName string, scopeName string, collectionName string) ([]string, error) {
	// TODO
	panic("In template_product, to be implemented")
}

func (p *Product) GenerateQueries(bucketName string, scopeName string, collectionName string) ([]string, error) {
	// TODO
	panic("In template_product, to be implemented")
}

func (p *Product) GenerateIndexesForSdk() (map[string][]string, error) {
	// TODO
	panic("In template_product, to be implemented")
}

func (p *Product) GenerateSubPathAndValue(fake *faker.Faker, subDocSize int) map[string]any {
	return map[string]interface{}{
		"_1": strings.Repeat(fake.Letter(), subDocSize),
	}
}
