// Express 4 ignores the promise an async handler returns, so a rejection in a
// handler without its own try/catch never reaches the error middleware and, on
// Node 15+, crashes the process. Forward those rejections to next(err) for every
// route and middleware, the same way Express 5 does.
const Layer = require("express/lib/router/layer");

const forwardRejection = (result, next) => {
  if (result && typeof result.then === "function") {
    result.then(undefined, (error) => next(error || new Error("Request handler rejected")));
  }
};

if (!Layer.prototype.__asyncErrorsPatched) {
  Layer.prototype.handle_request = function handle(req, res, next) {
    const fn = this.handle;
    if (fn.length > 3) return next();

    try {
      forwardRejection(fn(req, res, next), next);
    } catch (error) {
      next(error);
    }
  };

  Layer.prototype.handle_error = function handleError(error, req, res, next) {
    const fn = this.handle;
    if (fn.length !== 4) return next(error);

    try {
      forwardRejection(fn(error, req, res, next), next);
    } catch (caught) {
      next(caught);
    }
  };

  Layer.prototype.__asyncErrorsPatched = true;
}
