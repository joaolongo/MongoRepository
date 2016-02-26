using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoRepository
{
    /// <summary>
    ///     Deals with entities in MongoDb.
    /// </summary>
    /// <typeparam name="T">The type contained in the repository.</typeparam>
    /// <typeparam name="TKey">The type used for the entity's Id.</typeparam>
    public class MongoRepository<T, TKey> : IRepository<T, TKey>
        where T : IEntity<TKey>
    {
        /// <summary>
        ///     MongoCollection field.
        /// </summary>
        protected internal IMongoCollection<T> collection;

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        ///     Uses the Default App/Web.Config connectionstrings to fetch the connectionString and Database name.
        /// </summary>
        /// <remarks>Default constructor defaults to "MongoServerSettings" key for connectionstring.</remarks>
        public MongoRepository()
            : this(Util<TKey>.GetDefaultConnectionString())
        {
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="connectionString">Connectionstring to use for connecting to MongoDB.</param>
        public MongoRepository(string connectionString)
        {
            collection = Util<TKey>.GetCollectionFromConnectionString<T>(connectionString);
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="connectionString">Connectionstring to use for connecting to MongoDB.</param>
        /// <param name="collectionName">The name of the collection to use.</param>
        public MongoRepository(string connectionString, string collectionName)
        {
            collection = Util<TKey>.GetCollectionFromConnectionString<T>(connectionString, collectionName);
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="url">Url to use for connecting to MongoDB.</param>
        public MongoRepository(MongoUrl url)
        {
            collection = Util<TKey>.GetCollectionFromUrl<T>(url);
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="url">Url to use for connecting to MongoDB.</param>
        /// <param name="collectionName">The name of the collection to use.</param>
        public MongoRepository(MongoUrl url, string collectionName)
        {
            collection = Util<TKey>.GetCollectionFromUrl<T>(url, collectionName);
        }

        /// <summary>
        ///     Gets the name of the collection
        /// </summary>
        public string CollectionName
        {
            get { return collection.CollectionNamespace.CollectionName; }
        }

        /// <summary>
        ///     Gets the Mongo collection (to perform advanced operations).
        /// </summary>
        /// <remarks>
        ///     One can argue that exposing this property (and with that, access to it's Database property for instance
        ///     (which is a "parent")) is not the responsibility of this class. Use of this property is highly discouraged;
        ///     for most purposes you can use the MongoRepositoryManager&lt;T&gt;
        /// </remarks>
        /// <value>The Mongo collection (to perform advanced operations).</value>
        public IMongoCollection<T> Collection
        {
            get { return collection; }
        }

        /// <summary>
        ///     Adds the new entity in the repository asynchronously.
        /// </summary>
        /// <param name="entity">The entity T.</param>
        /// <returns>The added entity including its new ObjectId.</returns>
        public virtual async Task<T> AddAsync(T entity)
        {
            await collection.InsertOneAsync(entity);
            return entity;
        }

        /// <summary>
        ///     Adds the new entities in the repository asynchronously.
        /// </summary>
        /// <param name="entities">The entities of type T.</param>
        public virtual async Task AddAsync(IEnumerable<T> entities)
        {
            await collection.InsertManyAsync(entities);
        }

        /// <summary>
        ///     Upserts an entity asynchronously.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>The updated entity.</returns>
        public virtual async Task<T> UpdateAsync(T entity)
        {
            if (entity.Id == null)
                await AddAsync(entity);
            else
                await collection.ReplaceOneAsync(GetIDFilter(entity.Id), entity, new UpdateOptions {IsUpsert = true});
            return entity;
        }

        /// <summary>
        ///     Upserts the entities asynchronously
        /// </summary>
        /// <param name="entities">The entities to update</param>
        public virtual async Task UpdateAsync(IEnumerable<T> entities)
        {
            foreach (var entity in entities)
                await collection.ReplaceOneAsync(GetIDFilter(entity.Id), entity, new UpdateOptions {IsUpsert = true});
        }

        /// <summary>
        ///     Deletes an entity from the repository by its ObjectId asynchronously.
        /// </summary>
        /// <param name="id">The ObjectId of the entity.</param>
        public virtual void DeleteAsync(TKey id)
        {
            var filter = Builders<T>.Filter.Where(x => x.Id.Equals(id));
            collection.DeleteOneAsync(filter);
        }

        /// <summary>
        ///     Deletes the given entity asynchronously.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        public virtual async Task DeleteAsync(T entity)
        {
            await DeleteAsync(x => x.Id.Equals(entity.Id));
        }

        /// <summary>
        ///     Deletes the entities matching the predicate asynchronously.
        /// </summary>
        /// <param name="predicate">The expression.</param>
        public virtual async Task DeleteAsync(Expression<Func<T, bool>> predicate)
        {
            var filter = Builders<T>.Filter.Where(predicate);
            await collection.DeleteManyAsync(filter);
        }

        /// <summary>
        ///     Deletes all entities in the repository asynchronously.
        /// </summary>
        public virtual async void DeleteAllAsync()
        {
            await collection.DeleteManyAsync(x => true);
        }

        /// <summary>
        ///     Returns the T by its given id.
        /// </summary>
        /// <param name="id">The Id of the entity to retrieve.</param>
        /// <returns>The Entity T.</returns>
        public virtual T GetById(TKey id)
        {
            return collection.FindSync<T>(GetIDFilter(id)).Single();
        }

        /// <summary>
        ///     Adds the new entity in the repository.
        /// </summary>
        /// <param name="entity">The entity T.</param>
        /// <returns>The added entity including its new ObjectId.</returns>
        public virtual T Add(T entity)
        {
            collection.InsertOne(entity);

            return entity;
        }

        /// <summary>
        ///     Adds the new entities in the repository.
        /// </summary>
        /// <param name="entities">The entities of type T.</param>
        public virtual void Add(IEnumerable<T> entities)
        {
            collection.InsertMany(entities);
        }

        /// <summary>
        ///     Upserts an entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>The updated entity.</returns>
        public virtual T Update(T entity)
        {
            if (entity.Id == null)
                Add(entity);
            else
                collection.ReplaceOne(GetIDFilter(entity.Id), entity, new UpdateOptions {IsUpsert = true});
            return entity;
        }

        /// <summary>
        ///     Upserts the entities.
        /// </summary>
        /// <param name="entities">The entities to update.</param>
        public virtual void Update(IEnumerable<T> entities)
        {
            foreach (var entity in entities)
                collection.ReplaceOne(GetIDFilter(entity.Id), entity, new UpdateOptions {IsUpsert = true});
        }

        /// <summary>
        ///     Deletes an entity from the repository by its id.
        /// </summary>
        /// <param name="id">The entity's id.</param>
        public virtual void Delete(TKey id)
        {
            collection.DeleteOne(GetIDFilter(id));
        }

        /// <summary>
        ///     Deletes the given entity.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        public virtual void Delete(T entity)
        {
            Delete(entity.Id);
        }

        /// <summary>
        ///     Deletes the entities matching the predicate.
        /// </summary>
        /// <param name="predicate">The expression.</param>
        public virtual void Delete(Expression<Func<T, bool>> predicate)
        {
            collection.DeleteMany<T>(predicate);
        }

        /// <summary>
        ///     Deletes all entities in the repository.
        /// </summary>
        public virtual void DeleteAll()
        {
            collection.DeleteMany(t => true);
        }

        /// <summary>
        ///     Counts the total entities in the repository.
        /// </summary>
        /// <returns>Count of entities in the collection.</returns>
        public virtual long Count()
        {
            return collection.Count(t => true);
        }

        /// <summary>
        ///     Checks if the entity exists for given predicate.
        /// </summary>
        /// <param name="predicate">The expression.</param>
        /// <returns>True when an entity matching the predicate exists, false otherwise.</returns>
        public virtual bool Exists(Expression<Func<T, bool>> predicate)
        {
            return collection.AsQueryable().Any(predicate);
        }

        /// <summary>
        ///     Returns the T by its given id.
        /// </summary>
        /// <param name="id">The Id of the entity to retrieve.</param>
        /// <returns>The Entity T.</returns>
        public virtual T GetById(ObjectId id)
        {
            return collection.FindSync<T>(GetIDFilter(id)).Single();
        }

        /// <summary>
        ///     Deletes an entity from the repository by its ObjectId.
        /// </summary>
        /// <param name="id">The ObjectId of the entity.</param>
        public virtual void Delete(ObjectId id)
        {
            collection.DeleteOne(GetIDFilter(id));
        }

        private static FilterDefinition<T> GetIDFilter(ObjectId id)
        {
            return Builders<T>.Filter.Eq("_id", id);
        }

        private static FilterDefinition<T> GetIDFilter(TKey id)
        {
            if (typeof (T).IsSubclassOf(typeof (Entity)))
                return GetIDFilter(new ObjectId(id as string));
            return Builders<T>.Filter.Eq("_id", id);
        }

        #region IQueryable<T>

        /// <summary>
        ///     Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>An IEnumerator&lt;T&gt; object that can be used to iterate through the collection.</returns>
        public virtual IEnumerator<T> GetEnumerator()
        {
            return collection.AsQueryable().GetEnumerator();
        }

        /// <summary>
        ///     Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>An IEnumerator object that can be used to iterate through the collection.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return collection.AsQueryable().GetEnumerator();
        }

        /// <summary>
        ///     Gets the type of the element(s) that are returned when the expression tree associated with this instance of
        ///     IQueryable is executed.
        /// </summary>
        public virtual Type ElementType
        {
            get { return collection.AsQueryable().ElementType; }
        }

        /// <summary>
        ///     Gets the expression tree that is associated with the instance of IQueryable.
        /// </summary>
        public virtual Expression Expression
        {
            get { return collection.AsQueryable().Expression; }
        }

        /// <summary>
        ///     Gets the query provider that is associated with this data source.
        /// </summary>
        public virtual IQueryProvider Provider
        {
            get { return collection.AsQueryable().Provider; }
        }

        #endregion
    }

    /// <summary>
    ///     Deals with entities in MongoDb.
    /// </summary>
    /// <typeparam name="T">The type contained in the repository.</typeparam>
    /// <remarks>Entities are assumed to use strings for Id's.</remarks>
    public class MongoRepository<T> : MongoRepository<T, string>, IRepository<T>
        where T : IEntity<string>
    {
        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        ///     Uses the Default App/Web.Config connectionstrings to fetch the connectionString and Database name.
        /// </summary>
        /// <remarks>Default constructor defaults to "MongoServerSettings" key for connectionstring.</remarks>
        public MongoRepository()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="url">Url to use for connecting to MongoDB.</param>
        public MongoRepository(MongoUrl url)
            : base(url)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="url">Url to use for connecting to MongoDB.</param>
        /// <param name="collectionName">The name of the collection to use.</param>
        public MongoRepository(MongoUrl url, string collectionName)
            : base(url, collectionName)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="connectionString">Connectionstring to use for connecting to MongoDB.</param>
        public MongoRepository(string connectionString)
            : base(connectionString)
        {
        }

        /// <summary>
        ///     Initializes a new instance of the MongoRepository class.
        /// </summary>
        /// <param name="connectionString">Connectionstring to use for connecting to MongoDB.</param>
        /// <param name="collectionName">The name of the collection to use.</param>
        public MongoRepository(string connectionString, string collectionName)
            : base(connectionString, collectionName)
        {
        }
    }
}