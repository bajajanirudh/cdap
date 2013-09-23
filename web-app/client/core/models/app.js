/*
 * App Model
 */

define([], function () {

	var Model = Em.Object.extend({
		type: 'App',
		plural: 'Apps',

		href: function () {
			return '#/apps/' + this.get('id');
		}.property(),

		init: function() {
			this._super();

			this.set('timeseries', Em.Object.create());
			this.set('aggregates', Em.Object.create());

			this.set('counts', {
				Stream: 0,
				Flow: 0,
				Batch: 0,
				Dataset: 0,
				Query: 0
			});

		},

		units: {
			'storage': 'bytes'
		},

		/*
		 * Runnable context path, used by user-defined metrics.
		 */
		context: function () {

			return this.interpolate('/apps/{id}');

		}.property('id'),

		interpolate: function (path) {

			return path.replace(/\{id\}/, this.get('id'));

		},

		trackMetric: function (path, kind, label) {

			this.get(kind).set(path = this.interpolate(path), label || []);
			return path;

		},

		setMetric: function (label, value) {

			var unit = this.get('units')[label];
			value = C.Util[unit](value);

			this.set(label + 'Label', value[0]);
			this.set(label + 'Units', value[1]);

		},

		getSubPrograms: function (callback, http) {

			var types = ['flows', 'mapreduces', 'procedures'];
			var remaining = types.length - 1, i = types.length;
			var result = {};
			var id = this.get('id');
			var kinds = {
				'flows': 'Flow',
				'mapreduces': 'Batch',
				'procedures': 'Procedure'
			};

			while (i--) {

				(function () {

					var type = types[i];

					http.rest('apps', id, type, function (models) {

						var j = models.length;
						while (j--) {
							models[j] = C[kinds[type]].create(models[j]);
						}

						result[kinds[type]] = models;

						if (!--remaining) {
							callback(result);
						}

					});

				})();

			}

		}

	});

	Model.reopenClass({
		type: 'App',
		kind: 'Model',
		find: function(model_id, http) {

			var promise = Ember.Deferred.create();

			http.rest('apps', model_id, function (model, error) {

				model = C.App.create(model);
				promise.resolve(model);

			});

			return promise;
		}
	});

	return Model;

});