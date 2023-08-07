package tasks

import (
	"fmt"
	"github.com/couchbaselabs/sirius/internal/sdk"
)

type RetryExceptions struct {
	IdentifierToken string     `json:"identifierToken" doc:"true"`
	ResultSeed      string     `json:"resultSeed" doc:"true"`
	Exceptions      Exceptions `json:"exceptions" doc:"true"`
	Task            Task       `json:"-" doc:"false"`
	req             *Request   `json:"-" doc:"false"`
}

func (r *RetryExceptions) Describe() string {
	return "Retry Exception reties failed operations.\n" +
		"IgnoreExceptions will ignore failed operation occurred in this category. \n" +
		"RetryExceptions will retry failed operation occurred in this category. \n" +
		"RetryAttempts is the number of retry attempts.\n"
}

func (r *RetryExceptions) Do() error {
	c, _ := r.Task.GetCollectionObject()
	r.Task.SetException(r.Exceptions)
	r.Task.PostTaskExceptionHandling(c)
	r.Task.tearUp()
	return nil
}

func (r *RetryExceptions) Config(req *Request, reRun bool) (int64, error) {
	r.req = req
	if r.req == nil {
		return 0, fmt.Errorf("request.Request struct is nil")
	}
	for i := range r.req.Tasks {

		if r.req.Tasks[i].Task.GetResultSeed() == r.ResultSeed {
			r.Task = r.req.Tasks[i].Task
			break
		}
	}
	return r.Task.Config(req, true)

}

func (r *RetryExceptions) BuildIdentifier() string {
	if r.IdentifierToken == "" {
		r.IdentifierToken = DefaultIdentifierToken
	}
	return r.IdentifierToken
}

func (r *RetryExceptions) CollectionIdentifier() string {
	return r.Task.CollectionIdentifier()
}

func (r *RetryExceptions) CheckIfPending() bool {
	return r.Task.CheckIfPending()
}

func (r *RetryExceptions) PostTaskExceptionHandling(_ *sdk.CollectionObject) {

}

func (r *RetryExceptions) tearUp() error {
	return r.Task.tearUp()
}
func (r *RetryExceptions) GetResultSeed() string {
	return r.Task.GetResultSeed()
}

func (r *RetryExceptions) GetCollectionObject() (*sdk.CollectionObject, error) {
	return r.Task.GetCollectionObject()
}

func (r RetryExceptions) SetException(exceptions Exceptions) {
	r.Task.SetException(r.Exceptions)
}
