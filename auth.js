module.exports.authorize = function (req, res, next) {

    console.log("req.headers",req.headers)

    console.log(req.headers.authorization)

    const b64auth = (req.headers.authorization || '').split(' ')[1] || '';

    const [username, password] = Buffer.from(b64auth, 'base64').toString().split(':');

    if (username !=="rxds" || password !=="1234") {

          res.json({msg:"Unauthorized"});

    } else {

      next();

    }

  };